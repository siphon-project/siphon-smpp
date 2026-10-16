//! The configs shipped in the repo load, and load to what they say.

use siphon_smpp::config::ThrottleAction;
use siphon_smpp::SmppConfig;
use std::path::PathBuf;

fn repo_file(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative)
}

#[test]
fn the_deploy_example_loads() {
    let cfg = SmppConfig::from_file(repo_file("deploy/smpp.example.yaml")).expect("loads");
    assert_eq!(cfg.listen(), ("0.0.0.0".to_string(), 2775));
    assert_eq!(cfg.server.session_init_timer_ms, 5000);
    assert_eq!(cfg.server.inactivity_timer_ms, 300_000);
    assert_eq!(cfg.server.max_msg_per_sec, 200);
    assert_eq!(cfg.server.throttle_action, ThrottleAction::Pace);
    let alpha = cfg.binds.iter().find(|b| b.name == "alpha").expect("alpha");
    assert_eq!(alpha.host, "smsc-a.example.net");
    assert_eq!(alpha.port, 2775);
    assert_eq!(alpha.bind_type, "transceiver");
    assert_eq!(alpha.max_msg_per_sec, 50);
    assert!(cfg.binds.iter().any(|b| b.name == "beta"));
    assert_eq!(cfg.routing.default_chain, ["bind:beta"]);
    assert_eq!(cfg.routing.rules.len(), 1);
    assert_eq!(cfg.routing.rules[0].prefix, "1555");
    assert_eq!(cfg.routing.rules[0].name, "north-america");
    assert_eq!(cfg.routing.rules[0].chain, ["bind:alpha"]);
}

#[test]
fn the_load_test_harness_config_loads() {
    let cfg = SmppConfig::from_file(repo_file("harness/smpp.yaml")).expect("loads");
    assert_eq!(cfg.listen(), ("0.0.0.0".to_string(), 2775));
    assert_eq!(cfg.server.enquire_link_timer_ms, 30_000);
    assert_eq!(cfg.server.response_timer_ms, 30_000);
}

/// The `smpp.yaml` key of the Kubernetes ConfigMap, with its block
/// indentation removed. Its `${VAR}` references have no defaults, so with
/// the variables unset they expand to empty values.
#[test]
fn the_kubernetes_configmap_smpp_yaml_loads() {
    let manifest = std::fs::read_to_string(repo_file("deploy/k8s/configmap.yaml")).expect("read");
    let mut lines = manifest.lines().skip_while(|l| l.trim() != "smpp.yaml: |");
    lines.next();
    let body: String = lines
        .take_while(|l| l.is_empty() || l.starts_with("    "))
        .map(|l| format!("{}\n", l.get(4..).unwrap_or("")))
        .collect();
    assert!(body.contains("binds:"), "extracted: {body}");

    let path = std::env::temp_dir().join("siphon_smpp_configmap_example.yaml");
    std::fs::write(&path, body).expect("write");
    let cfg = SmppConfig::from_file(&path).expect("loads");
    std::fs::remove_file(&path).ok();

    assert_eq!(cfg.server.max_msg_per_sec, 200);
    assert_eq!(cfg.binds[0].name, "alpha");
    assert_eq!(cfg.binds[0].max_msg_per_sec, 50);
    assert_eq!(cfg.routing.default_chain, ["bind:alpha"]);
}
