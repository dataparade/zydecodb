//! Shared handle aliases used across server dispatch paths.

use std::sync::{Arc, RwLock};
use zydecodb_document::catalog::Catalog;
use zydecodb_engine::engine_handle::EngineHandle;

pub type SharedEngine = Arc<EngineHandle>;
pub type SharedCatalog = Arc<RwLock<Catalog>>;
