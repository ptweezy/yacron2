//! Error types shared across the crate.

use std::fmt;

/// A configuration error.
///
/// Every problem discovered while loading, parsing, merging, or validating
/// configuration is funnelled into this single type so that `main` can report
/// it uniformly and exit non-zero, exactly like the original `ConfigError`.
#[derive(Debug, Clone)]
pub struct ConfigError(pub String);

impl ConfigError {
    pub fn new(msg: impl Into<String>) -> Self {
        ConfigError(msg.into())
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

/// Convenience constructor: `cfg_err!("unknown timezone: {tz}")`.
macro_rules! cfg_err {
    ($($arg:tt)*) => {
        $crate::error::ConfigError::new(format!($($arg)*))
    };
}

pub(crate) use cfg_err;

pub type ConfigResult<T> = Result<T, ConfigError>;
