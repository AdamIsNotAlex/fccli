//! Shared library surface for fccli.

#[cfg(all(feature = "test-transport", feature = "production-transport"))]
compile_error!("features `test-transport` and `production-transport` are mutually exclusive");

pub mod app;
pub mod chart;
pub mod cli;
pub mod clock;
pub mod error;
pub mod history;
pub mod model;
pub mod provider;
pub mod snapshot;
pub mod terminal;
