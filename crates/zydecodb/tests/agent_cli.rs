//! CLI contract for `zydecodb --agent`.

use std::process::Command;
use zydecodb::agent::{INDEX_MAX_BYTES, TOPICS, TOPIC_MAX_BYTES};

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_zydecodb"))
}

#[test]
fn agent_flag_prints_index() {
    let out = bin().arg("--agent").output().expect("run --agent");
    assert!(
        out.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert!(stdout.starts_with("zydecodb-agent-docs: 1\n"));
    assert!(stdout.contains(&format!("zydecodb: {}", env!("CARGO_PKG_VERSION"))));
    assert!(stdout.contains("topic: index"));
    for topic in TOPICS {
        if *topic != "index" {
            assert!(
                stdout.contains(&format!("`{topic}`")),
                "index missing topic {topic}"
            );
        }
    }
    assert!(stdout.len() <= INDEX_MAX_BYTES);
    assert!(
        out.stderr.is_empty(),
        "stderr must stay empty: {:?}",
        out.stderr
    );
}

#[test]
fn each_topic_exits_zero_under_cap() {
    for topic in TOPICS {
        let out = bin()
            .args(["--agent", topic])
            .output()
            .unwrap_or_else(|e| panic!("--agent {topic}: {e}"));
        assert!(
            out.status.success(),
            "--agent {topic} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let stdout = String::from_utf8(out.stdout).unwrap();
        let cap = if *topic == "index" {
            INDEX_MAX_BYTES
        } else {
            TOPIC_MAX_BYTES
        };
        assert!(
            stdout.len() <= cap,
            "--agent {topic} is {} bytes (cap {cap})",
            stdout.len()
        );
        assert!(stdout.contains(&format!("topic: {topic}")));
    }
}

#[test]
fn unknown_topic_exits_two() {
    let out = bin().args(["--agent", "not-a-topic"]).output().unwrap();
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.contains("unknown agent topic"));
    for topic in TOPICS {
        assert!(stderr.contains(topic), "stderr missing {topic}");
    }
    assert!(out.stdout.is_empty());
}

#[test]
fn help_mentions_agent_flag() {
    let out = bin().arg("--help").output().unwrap();
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert!(
        stdout.contains("zydecodb --agent"),
        "help missing agent pointer:\n{stdout}"
    );
}

#[test]
fn serve_agent_prints_index_and_exits() {
    let out = bin().args(["serve", "--agent"]).output().unwrap();
    assert!(
        out.status.success(),
        "serve --agent failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert!(stdout.contains("topic: index"));
    assert!(stdout.contains("zydecodb-agent-docs: 1"));
}
