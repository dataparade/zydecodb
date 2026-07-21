use proptest::prelude::*;
use zydecodb::security::{SessionState, check_key_prefix_acl, check_collection_prefix_acl};

proptest! {
    #[test]
    fn prop_key_prefix_acl(
        prefixes in prop::collection::vec(any::<String>(), 0..5),
        key in any::<Vec<u8>>()
    ) {
        let mut session = SessionState::anonymous();
        session.allowed_prefixes = prefixes.clone();
        
        let result = check_key_prefix_acl(&session, &key);
        
        if prefixes.is_empty() {
            assert!(result.is_none(), "Empty prefixes should allow all keys");
        } else {
            let mut allowed = false;
            for prefix in &prefixes {
                if key.starts_with(prefix.as_bytes()) {
                    allowed = true;
                    break;
                }
            }
            
            if allowed {
                assert!(result.is_none(), "Key starting with prefix should be allowed");
            } else {
                assert!(result.is_some(), "Key not starting with prefix should be forbidden");
            }
        }
    }

    #[test]
    fn prop_collection_prefix_acl(
        prefixes in prop::collection::vec(any::<String>(), 0..5),
        collection in any::<String>()
    ) {
        let mut session = SessionState::anonymous();
        session.allowed_prefixes = prefixes.clone();
        
        let result = check_collection_prefix_acl(&session, &collection);
        
        if prefixes.is_empty() {
            assert!(result.is_none(), "Empty prefixes should allow all collections");
        } else {
            let mut allowed = false;
            for prefix in &prefixes {
                let stripped = prefix.strip_suffix(':').unwrap_or(prefix);
                if collection.starts_with(prefix) || collection == stripped {
                    allowed = true;
                    break;
                }
            }
            
            if allowed {
                assert!(result.is_none(), "Collection matching prefix rules should be allowed");
            } else {
                assert!(result.is_some(), "Collection not matching prefix rules should be forbidden");
            }
        }
    }
}
