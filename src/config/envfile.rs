//! Parsing of `env_file` files (a list of `KEY=VALUE` pairs).
//!
//! Comments (`#`) and blank lines are ignored; surrounding spaces are trimmed;
//! only the first `=` separates key from value (so values may contain `=`).
//! Both error messages mention "env_file", matching the original.

use crate::error::{cfg_err, ConfigResult};

/// Parse an env file into ordered `(key, value)` pairs (file order preserved).
pub fn parse(path: &str) -> ConfigResult<Vec<(String, String)>> {
    let contents =
        std::fs::read_to_string(path).map_err(|e| cfg_err!("Could not load env_file: {e}"))?;

    let mut out = Vec::new();
    for raw in contents.lines() {
        let line = raw.trim_matches(' ');
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| cfg_err!("Invalid line in env_file: '{line}'"))?;
        out.push((
            key.trim_matches(' ').to_string(),
            value.trim_matches(' ').to_string(),
        ));
    }
    Ok(out)
}
