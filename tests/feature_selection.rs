fn active_transport_features() -> (bool, bool) {
    (
        cfg!(feature = "production-transport"),
        cfg!(feature = "test-transport"),
    )
}

fn declared_default_features(manifest: &str) -> Option<&str> {
    let (_, features_and_beyond) = manifest.split_once("[features]")?;
    let features = features_and_beyond
        .split_once("\n[")
        .map_or(features_and_beyond, |(section, _)| section);

    features.lines().find_map(|line| {
        line.trim()
            .strip_prefix("default")?
            .trim_start()
            .strip_prefix('=')
            .map(str::trim)
    })
}

#[test]
fn default_is_production_only() {
    assert_eq!(
        declared_default_features(include_str!("../Cargo.toml")),
        Some(r#"["production-transport"]"#),
        "Cargo's default feature set must contain only `production-transport`"
    );

    #[cfg(feature = "production-transport")]
    assert_eq!(
        active_transport_features(),
        (true, false),
        "the default build must enable only `production-transport`"
    );
}

#[cfg(feature = "test-transport")]
#[test]
fn test_transport_is_independent() {
    assert_eq!(
        active_transport_features(),
        (false, true),
        "the independent test mode must enable only `test-transport`"
    );
}
