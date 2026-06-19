//! Jinja2-style templating for report subjects/bodies (via `minijinja`), plus
//! the simpler `str.format`-style rendering used for stream-line prefixes.

use minijinja::{Environment, Value};

/// Render a Jinja2 template `source` against `context`.
///
/// minijinja honours the `{%-`/`-%}` whitespace-control markers used by the
/// default templates, matching the original jinja2 output closely.
pub fn render(source: &str, context: Value) -> Result<String, minijinja::Error> {
    let env = Environment::new();
    env.render_str(source, context)
}

/// Expand the `{job_name}` / `{stream_name}` placeholders in a stream prefix.
///
/// This mirrors `streamPrefix.format(job_name=..., stream_name=...)`; only the
/// two documented placeholders are substituted.
pub fn format_stream_prefix(prefix: &str, job_name: &str, stream_name: &str) -> String {
    prefix
        .replace("{job_name}", job_name)
        .replace("{stream_name}", stream_name)
}
