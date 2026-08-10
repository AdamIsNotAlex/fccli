//! Shared library surface for fccli.

#[cfg(all(feature = "test-transport", feature = "production-transport"))]
compile_error!("features `test-transport` and `production-transport` are mutually exclusive");

pub mod chart;
pub mod error;
pub mod model;
pub mod provider;
