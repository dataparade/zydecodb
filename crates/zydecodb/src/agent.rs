//! Embedded agent-facing usage contract (`zydecodb --agent [TOPIC]`).

/// Byte cap for the default index page.
pub const INDEX_MAX_BYTES: usize = 12_000;
/// Byte cap for every non-index topic page.
pub const TOPIC_MAX_BYTES: usize = 16_000;

/// Known topics, including the default `index`.
pub const TOPICS: &[&str] = &[
    "index",
    "python",
    "go",
    "typescript",
    "query",
    "kv",
    "tx",
    "watch",
    "aggregate",
    "ops",
    "pitfalls",
];

/// Return the markdown page for `topic` (`index` is the default page).
pub fn render(topic: &str) -> Result<&'static str, String> {
    let key = topic.trim().to_ascii_lowercase();
    let page = match key.as_str() {
        "index" => include_str!("../../../docs/agent/INDEX.md"),
        "python" => include_str!("../../../docs/agent/python.md"),
        "go" => include_str!("../../../docs/agent/go.md"),
        "typescript" => include_str!("../../../docs/agent/typescript.md"),
        "query" => include_str!("../../../docs/agent/query.md"),
        "kv" => include_str!("../../../docs/agent/kv.md"),
        "tx" => include_str!("../../../docs/agent/tx.md"),
        "watch" => include_str!("../../../docs/agent/watch.md"),
        "aggregate" => include_str!("../../../docs/agent/aggregate.md"),
        "ops" => include_str!("../../../docs/agent/ops.md"),
        "pitfalls" => include_str!("../../../docs/agent/pitfalls.md"),
        other => {
            return Err(format!(
                "unknown agent topic {other:?}\nknown topics: {}",
                TOPICS.join(", ")
            ));
        }
    };
    debug_assert!(
        page.len()
            <= if key == "index" {
                INDEX_MAX_BYTES
            } else {
                TOPIC_MAX_BYTES
            },
        "agent topic {key} is {} bytes (cap {})",
        page.len(),
        if key == "index" {
            INDEX_MAX_BYTES
        } else {
            TOPIC_MAX_BYTES
        }
    );
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header_ok(page: &str, topic: &str) {
        assert!(
            page.starts_with("zydecodb-agent-docs: 1\n"),
            "{topic}: missing docs version header"
        );
        assert!(
            page.contains(&format!("zydecodb: {}", env!("CARGO_PKG_VERSION"))),
            "{topic}: missing zydecodb version"
        );
        assert!(
            page.contains(&format!("topic: {topic}")),
            "{topic}: missing topic header"
        );
    }

    #[test]
    fn every_topic_renders_under_cap() {
        for topic in TOPICS {
            let page = render(topic).expect(topic);
            header_ok(page, topic);
            let cap = if *topic == "index" {
                INDEX_MAX_BYTES
            } else {
                TOPIC_MAX_BYTES
            };
            assert!(
                page.len() <= cap,
                "{topic} is {} bytes (cap {cap})",
                page.len()
            );
        }
    }

    #[test]
    fn unknown_topic_lists_known() {
        let err = render("not-a-topic").unwrap_err();
        assert!(err.contains("unknown agent topic"));
        for topic in TOPICS {
            assert!(err.contains(topic), "error missing {topic}: {err}");
        }
    }

    #[test]
    fn python_covers_driver_api_map() {
        let page = render("python").unwrap();
        for needle in [
            "Client",
            "collection",
            "insert_one",
            "insert_many",
            "replace_one",
            "replace_one_if_match",
            "update_one",
            "update_many",
            "update_by_id_if_match",
            "find",
            "find_one",
            "find_with_revision",
            "get",
            "get_with_revision",
            "aggregate",
            "watch",
            "transaction()",
            "generate_id",
        ] {
            assert!(page.contains(needle), "python.md missing {needle}");
        }
    }

    #[test]
    fn go_covers_driver_api_map() {
        let page = render("go").unwrap();
        for needle in [
            "NewClient",
            "Collection",
            "InsertOne",
            "InsertMany",
            "ReplaceOne",
            "ReplaceOneIfMatch",
            "UpdateOne",
            "UpdateMany",
            "UpdateByIDIfMatch",
            "Find",
            "FindOne",
            "FindWithRevision",
            "Get",
            "GetWithRevision",
            "Aggregate",
            "Watch",
            "Next",
            "BeginTx",
            "WithTransaction",
            "GenerateID",
        ] {
            assert!(page.contains(needle), "go.md missing {needle}");
        }
    }

    #[test]
    fn typescript_covers_driver_api_map() {
        let page = render("typescript").unwrap();
        for needle in [
            "Client",
            "collection",
            "insertOne",
            "insertMany",
            "replaceOne",
            "replaceOneIfMatch",
            "updateOne",
            "updateMany",
            "updateByIdIfMatch",
            "find",
            "findOne",
            "findWithRevision",
            "get",
            "getWithRevision",
            "aggregate",
            "watch",
            "withTransaction",
            "generateId",
        ] {
            assert!(page.contains(needle), "typescript.md missing {needle}");
        }
    }

    #[test]
    fn pitfalls_covers_not_yet() {
        let page = render("pitfalls").unwrap();
        for needle in ["$unwind", "arrayFilters", "SchemaDef"] {
            assert!(page.contains(needle), "pitfalls.md missing {needle}");
        }
    }
}
