//! A YAML scalar that is always captured as a string.
//!
//! strictyaml treated everything as text, so `value: 8080` and `month: 7` were
//! strings. serde would otherwise reject an integer where a `String` is
//! expected, so this type accepts any scalar and stringifies it.

use serde::de::{Deserializer, Visitor};
use serde::Deserialize;
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScalarString(pub String);

impl<'de> Deserialize<'de> for ScalarString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ScalarVisitor;

        impl<'de> Visitor<'de> for ScalarVisitor {
            type Value = ScalarString;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a string, number, or boolean")
            }

            fn visit_str<E>(self, v: &str) -> Result<ScalarString, E> {
                Ok(ScalarString(v.to_string()))
            }

            fn visit_string<E>(self, v: String) -> Result<ScalarString, E> {
                Ok(ScalarString(v))
            }

            fn visit_i64<E>(self, v: i64) -> Result<ScalarString, E> {
                Ok(ScalarString(v.to_string()))
            }

            fn visit_u64<E>(self, v: u64) -> Result<ScalarString, E> {
                Ok(ScalarString(v.to_string()))
            }

            fn visit_f64<E>(self, v: f64) -> Result<ScalarString, E> {
                Ok(ScalarString(v.to_string()))
            }

            fn visit_bool<E>(self, v: bool) -> Result<ScalarString, E> {
                Ok(ScalarString(v.to_string()))
            }
        }

        deserializer.deserialize_any(ScalarVisitor)
    }
}
