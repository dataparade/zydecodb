use argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum KeyError {
    #[error("io error: {0}")]
    Io(String),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("invalid tenant hex: {0}")]
    InvalidTenant(String),
    #[error("key not found: {0}")]
    NotFound(String),
    #[error("key already exists: {0}")]
    AlreadyExists(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum KeyRole {
    ReadOnly,
    #[default]
    ReadWrite,
    Admin,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct KeyRecord {
    pub id: String,
    pub secret_hash: String,
    #[serde(default)]
    pub role: KeyRole,
    #[serde(default = "default_tenant_hex")]
    pub tenant: String,
    #[serde(default)]
    pub allowed_prefixes: Vec<String>,
}

fn default_tenant_hex() -> String {
    "00000000000000000000000000000000".to_string()
}

/// Per-tenant resource limits, stored as `[[tenant]]` tables alongside `[[key]]`
/// entries in the same keys file. A tenant is identified by its 32-hex id; each
/// limit is optional (absent = unlimited / fall back to the global default).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TenantRecord {
    pub tenant: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_rps: Option<u32>,
}

#[derive(Debug, Deserialize, Serialize)]
struct KeysFile {
    #[serde(default)]
    key: Vec<KeyRecord>,
    #[serde(default)]
    tenant: Vec<TenantRecord>,
}

#[derive(Debug, Clone)]
pub struct KeyStore {
    path: PathBuf,
    records: Vec<KeyRecord>,
    tenants: Vec<TenantRecord>,
    bootstrap_secret: Option<String>,
}

impl KeyStore {
    pub fn load(path: &Path) -> Result<Self, KeyError> {
        let bootstrap_secret = std::env::var("ZYDECODB_BOOTSTRAP_KEY").ok();
        if bootstrap_secret.is_some() {
            tracing::warn!(
                "ZYDECODB_BOOTSTRAP_KEY is set — dev-only bootstrap auth enabled; do not use in production"
            );
        }

        let (records, tenants) = if path.exists() {
            let text = fs::read_to_string(path).map_err(|e| KeyError::Io(e.to_string()))?;
            let file: KeysFile =
                toml::from_str(&text).map_err(|e| KeyError::Parse(e.to_string()))?;
            (file.key, file.tenant)
        } else {
            (Vec::new(), Vec::new())
        };

        Ok(KeyStore {
            path: path.to_path_buf(),
            records,
            tenants,
            bootstrap_secret,
        })
    }

    /// The configured per-tenant limit records (for building [`TenantLimits`]).
    pub fn tenant_records(&self) -> &[TenantRecord] {
        &self.tenants
    }

    /// Upsert a tenant's limits and persist. `max_bytes`/`rate_rps` of `None`
    /// leave that limit unchanged for an existing tenant (or unset for a new one).
    pub fn set_tenant_limit(
        path: &Path,
        tenant_hex: &str,
        max_bytes: Option<u64>,
        rate_rps: Option<u32>,
    ) -> Result<(), KeyError> {
        parse_tenant_hex(tenant_hex)?;
        let mut store = KeyStore::load(path)?;
        match store.tenants.iter_mut().find(|t| t.tenant == tenant_hex) {
            Some(existing) => {
                if max_bytes.is_some() {
                    existing.max_bytes = max_bytes;
                }
                if rate_rps.is_some() {
                    existing.rate_rps = rate_rps;
                }
            }
            None => store.tenants.push(TenantRecord {
                tenant: tenant_hex.to_string(),
                max_bytes,
                rate_rps,
            }),
        }
        store.save()
    }

    /// List configured tenant limits as `(tenant_hex, max_bytes, rate_rps)`.
    pub fn list_tenant_limits(&self) -> Vec<(String, Option<u64>, Option<u32>)> {
        self.tenants
            .iter()
            .map(|t| (t.tenant.clone(), t.max_bytes, t.rate_rps))
            .collect()
    }

    pub fn verify(&self, secret: &[u8]) -> Option<KeyRecord> {
        if let Some(ref bootstrap) = self.bootstrap_secret {
            if constant_time_eq(secret, bootstrap.as_bytes()) {
                return Some(KeyRecord {
                    id: "bootstrap".to_string(),
                    secret_hash: String::new(),
                    role: KeyRole::Admin,
                    tenant: default_tenant_hex(),
                    allowed_prefixes: vec![],
                });
            }
        }

        let secret_str = std::str::from_utf8(secret).ok()?;
        for record in &self.records {
            let parsed = PasswordHash::new(&record.secret_hash).ok()?;
            if Argon2::default()
                .verify_password(secret_str.as_bytes(), &parsed)
                .is_ok()
            {
                return Some(record.clone());
            }
        }
        None
    }

    pub fn list_ids(&self) -> Vec<String> {
        self.records.iter().map(|r| r.id.clone()).collect()
    }

    pub fn create_key(
        path: &Path,
        id: &str,
        role: KeyRole,
        tenant_hex: &str,
        allowed_prefixes: Vec<String>,
    ) -> Result<String, KeyError> {
        let mut store = KeyStore::load(path)?;
        if store.records.iter().any(|r| r.id == id) {
            return Err(KeyError::AlreadyExists(id.to_string()));
        }
        parse_tenant_hex(tenant_hex)?;

        let secret = generate_api_key();
        let hash = hash_secret(&secret)?;

        store.records.push(KeyRecord {
            id: id.to_string(),
            secret_hash: hash,
            role,
            tenant: tenant_hex.to_string(),
            allowed_prefixes,
        });
        store.save()?;
        Ok(secret)
    }

    pub fn revoke_key(path: &Path, id: &str) -> Result<(), KeyError> {
        let mut store = KeyStore::load(path)?;
        let before = store.records.len();
        store.records.retain(|r| r.id != id);
        if store.records.len() == before {
            return Err(KeyError::NotFound(id.to_string()));
        }
        store.save()
    }

    fn save(&self) -> Result<(), KeyError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|e| KeyError::Io(e.to_string()))?;
        }
        let file = KeysFile {
            key: self.records.clone(),
            tenant: self.tenants.clone(),
        };
        let text = toml::to_string_pretty(&file).map_err(|e| KeyError::Parse(e.to_string()))?;
        fs::write(&self.path, text).map_err(|e| KeyError::Io(e.to_string()))
    }
}

impl KeyRecord {
    pub fn tenant_bytes(&self) -> Result<[u8; 16], KeyError> {
        parse_tenant_hex(&self.tenant)
    }
}

pub fn parse_tenant_hex(hex: &str) -> Result<[u8; 16], KeyError> {
    if hex.len() != 32 {
        return Err(KeyError::InvalidTenant(format!(
            "expected 32 hex chars, got {}",
            hex.len()
        )));
    }
    let mut out = [0u8; 16];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let s = std::str::from_utf8(chunk).map_err(|e| KeyError::InvalidTenant(e.to_string()))?;
        out[i] = u8::from_str_radix(s, 16).map_err(|e| KeyError::InvalidTenant(e.to_string()))?;
    }
    Ok(out)
}

pub fn hash_secret(secret: &str) -> Result<String, KeyError> {
    let salt = SaltString::generate(&mut OsRng);
    let hash = Argon2::default()
        .hash_password(secret.as_bytes(), &salt)
        .map_err(|e| KeyError::Parse(e.to_string()))?;
    Ok(hash.to_string())
}

fn generate_api_key() -> String {
    let bytes: [u8; 24] = rand::random();
    format!("zdk_{}", hex::encode(bytes))
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

// Minimal hex encode without adding hex crate — use format!
mod hex {
    pub fn encode(bytes: [u8; 24]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn create_verify_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("keys.toml");
        let secret = KeyStore::create_key(
            &path,
            "test",
            KeyRole::ReadWrite,
            &default_tenant_hex(),
            vec![],
        )
        .unwrap();
        let store = KeyStore::load(&path).unwrap();
        assert!(store.verify(secret.as_bytes()).is_some());
        assert!(store.verify(b"wrong").is_none());
    }
}
