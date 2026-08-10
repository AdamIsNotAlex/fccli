#[cfg(feature = "production-transport")]
#[test]
fn production_transport_is_selected_exclusively() {
    assert!(
        cfg!(feature = "production-transport"),
        "the production build must enable `production-transport`"
    );
    assert!(
        !cfg!(feature = "test-transport"),
        "the production build must not enable `test-transport`"
    );
}

#[cfg(feature = "test-transport")]
#[test]
fn test_transport_is_selected_exclusively() {
    assert!(
        cfg!(feature = "test-transport"),
        "the test-transport build must enable `test-transport`"
    );
    assert!(
        !cfg!(feature = "production-transport"),
        "the test-transport build must not enable `production-transport`"
    );
}
