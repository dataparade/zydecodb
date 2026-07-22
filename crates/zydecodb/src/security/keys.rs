use argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
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
    /// SHA-256 (hex) of the full secret, used as an O(1) index into the
    /// keystore so auth performs exactly one argon2 verify. The argon2
    /// `secret_hash` remains the actual credential check; this field only
    /// selects which record to verify against. Absent on keys minted before
    /// this field existed (those fall back to a linear scan — reissue them).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_lookup: Option<String>,
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
    /// sha256(secret) hex -> index into `records`, for O(1) auth lookup.
    lookup: HashMap<String, usize>,
}

impl KeyStore {
    pub fn load(path: &Path) -> Result<Self, KeyError> {
        #[cfg(feature = "failpoints")]
        fail::fail_point!("keystore_load_io_error", |_| Err(KeyError::Io(
            "simulated I/O error".into()
        )));

        let bootstrap_secret = std::env::var("ZYDECODB_BOOTSTRAP_KEY").ok();
        if bootstrap_secret.is_some() {
            tracing::warn!(
                "ZYDECODB_BOOTSTRAP_KEY is set — loopback/dev-only bootstrap auth; \
                 server refuses to start when listen is not loopback"
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

        let mut lookup = HashMap::with_capacity(records.len());
        let mut legacy_count = 0usize;
        for (i, record) in records.iter().enumerate() {
            match &record.secret_lookup {
                Some(l) => {
                    lookup.insert(l.to_ascii_lowercase(), i);
                }
                None => legacy_count += 1,
            }
        }
        if legacy_count > 0 {
            tracing::warn!(
                count = legacy_count,
                "keys without secret_lookup use a slow linear auth scan — \
                 reissue them with `admin keys create` to get O(1) lookup"
            );
        }

        Ok(KeyStore {
            path: path.to_path_buf(),
            records,
            tenants,
            bootstrap_secret,
            lookup,
        })
    }

    /// Number of key records loaded (excludes the env bootstrap key).
    pub fn record_count(&self) -> usize {
        self.records.len()
    }

    /// Whether a dev-only bootstrap secret is configured via env.
    pub fn has_bootstrap(&self) -> bool {
        self.bootstrap_secret.is_some()
    }

    /// All key records (read-only view, for startup policy checks).
    pub fn records(&self) -> &[KeyRecord] {
        &self.records
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
                    secret_lookup: None,
                    role: KeyRole::Admin,
                    tenant: default_tenant_hex(),
                    allowed_prefixes: vec![],
                });
            }
        }

        let secret_str = std::str::from_utf8(secret).ok()?;

        // Fast path: O(1) index by sha256(secret), then exactly one argon2
        // verify. The argon2 hash remains the credential; the index only
        // selects the candidate record.
        if let Some(&idx) = self.lookup.get(&secret_lookup_hex(secret_str)) {
            let record = &self.records[idx];
            if argon2_matches(secret_str, &record.secret_hash) {
                return Some(record.clone());
            }
            return None;
        }

        // Fallback for keys minted before secret_lookup existed: linear scan
        // over only those legacy records.
        for record in self.records.iter().filter(|r| r.secret_lookup.is_none()) {
            if argon2_matches(secret_str, &record.secret_hash) {
                return Some(record.clone());
            }
        }
        None
    }

    /// Checks if a previously authenticated session is still valid against the
    /// current keystore (e.g. after a SIGHUP reload).
    pub fn is_session_valid(&self, session: &crate::security::SessionState) -> bool {
        if !session.authenticated {
            return false;
        }

        let Some(key_id) = &session.key_id else {
            return false;
        };

        if key_id == "bootstrap" {
            return self.bootstrap_secret.is_some();
        }

        let Some(secret_hash) = &session.secret_hash else {
            return false;
        };

        // Find the key by ID and ensure the secret hash matches exactly.
        // If the hash changed, the key was revoked and recreated with the same ID.
        self.records
            .iter()
            .any(|r| r.id == *key_id && r.secret_hash == *secret_hash)
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
            secret_lookup: Some(secret_lookup_hex(&secret)),
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

/// SHA-256 of the secret, lower-case hex — the O(1) keystore index value.
pub fn secret_lookup_hex(secret: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    let digest = hasher.finalize();
    let mut s = String::with_capacity(64);
    use std::fmt::Write;
    for b in digest {
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn argon2_matches(secret: &str, stored_hash: &str) -> bool {
    let Ok(parsed) = PasswordHash::new(stored_hash) else {
        return false;
    };
    Argon2::default()
        .verify_password(secret.as_bytes(), &parsed)
        .is_ok()
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

    #[test]
    fn lookup_index_selects_correct_record_among_many() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("keys.toml");
        let s1 = KeyStore::create_key(&path, "a", KeyRole::ReadOnly, &default_tenant_hex(), vec![])
            .unwrap();
        let s2 = KeyStore::create_key(
            &path,
            "b",
            KeyRole::ReadWrite,
            &default_tenant_hex(),
            vec![],
        )
        .unwrap();
        let store = KeyStore::load(&path).unwrap();
        assert_eq!(store.verify(s1.as_bytes()).unwrap().id, "a");
        assert_eq!(store.verify(s2.as_bytes()).unwrap().id, "b");
        assert!(store.verify(b"zdk_not_a_real_key").is_none());
    }

    #[test]
    fn legacy_record_without_lookup_still_verifies() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("keys.toml");
        let secret = KeyStore::create_key(
            &path,
            "old",
            KeyRole::ReadWrite,
            &default_tenant_hex(),
            vec![],
        )
        .unwrap();
        // Strip secret_lookup from the file to simulate a pre-upgrade key.
        let text = std::fs::read_to_string(&path).unwrap();
        let stripped: String = text
            .lines()
            .filter(|l| !l.trim_start().starts_with("secret_lookup"))
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(&path, stripped).unwrap();

        let store = KeyStore::load(&path).unwrap();
        assert_eq!(store.verify(secret.as_bytes()).unwrap().id, "old");
        assert!(store.verify(b"wrong").is_none());
    }
}
