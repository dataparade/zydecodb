use super::keys::{KeyRecord, KeyRole};

/// Per-connection authentication and tenant context.
#[derive(Debug, Clone)]
pub struct SessionState {
    pub authenticated: bool,
    pub key_id: Option<String>,
    pub role: Option<KeyRole>,
    pub tenant: [u8; 16],
    pub allowed_prefixes: Vec<String>,
}

impl SessionState {
    pub fn anonymous() -> Self {
        SessionState {
            authenticated: false,
            key_id: None,
            role: None,
            tenant: [0u8; 16],
            allowed_prefixes: vec![],
        }
    }

    pub fn from_key_record(record: &KeyRecord) -> Self {
        let tenant = record.tenant_bytes().unwrap_or([0u8; 16]);
        SessionState {
            authenticated: true,
            key_id: Some(record.id.clone()),
            role: Some(record.role),
            tenant,
            allowed_prefixes: record.allowed_prefixes.clone(),
        }
    }

    pub fn is_admin(&self) -> bool {
        self.role == Some(KeyRole::Admin)
    }
}
