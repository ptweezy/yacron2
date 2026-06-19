//! The single source of truth for the program version.
//!
//! The value tracks the crate version declared in `Cargo.toml`. The release
//! pipeline can override it at build time by setting `YACRON2_BUILD_VERSION`
//! (so the published binary reports the computed release version without a
//! committed file change). It is exposed on the CLI (`--version`) and via the
//! web API (`GET /version`).

/// The yacron2 version string, e.g. `"1.0.4"`.
pub const VERSION: &str = match option_env!("YACRON2_BUILD_VERSION") {
    Some(version) => version,
    None => env!("CARGO_PKG_VERSION"),
};
