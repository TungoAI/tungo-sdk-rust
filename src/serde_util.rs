//! Serde helpers shared by the generated models.

/// Serde adapter for a field that distinguishes "absent" from an explicit `null`.
///
/// Use it on an `Option<Option<T>>` field together with
/// `#[serde(default, skip_serializing_if = "Option::is_none")]`:
///
/// | Rust value      | JSON           |
/// | --------------- | -------------- |
/// | `None`          | field omitted  |
/// | `Some(None)`    | `"field": null` |
/// | `Some(Some(v))` | `"field": v`   |
pub mod nullable {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    /// Serializes `Some(None)` as `null` and `Some(Some(v))` as `v`.
    pub fn serialize<T, S>(value: &Option<Option<T>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: Serialize,
        S: Serializer,
    {
        match value {
            Some(Some(inner)) => inner.serialize(serializer),
            _ => serializer.serialize_none(),
        }
    }

    /// Deserializes a present field: `null` becomes `Some(None)`, a value becomes `Some(Some(v))`.
    pub fn deserialize<'de, T, D>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
    where
        T: Deserialize<'de>,
        D: Deserializer<'de>,
    {
        Option::<T>::deserialize(deserializer).map(Some)
    }
}

/// Declares an open string enumeration: the known values of the contract plus
/// `Unknown(String)` for values added by a newer API version.
macro_rules! string_enum {
    (
        $(#[$meta:meta])*
        $name:ident {
            $($(#[$vmeta:meta])* $variant:ident = $value:literal,)*
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        #[non_exhaustive]
        pub enum $name {
            $($(#[$vmeta])* $variant,)*
            /// A value this version of the SDK does not know.
            Unknown(String),
        }

        /// The default is `Unknown` with an empty value. It only stands for
        /// "not chosen yet" in a model built with `..Default::default()`: it
        /// is never produced by decoding and cannot be sent (serializing it
        /// is an error), so a required enumeration cannot go out empty.
        impl Default for $name {
            fn default() -> Self {
                Self::Unknown(String::new())
            }
        }

        impl $name {
            /// The value as it is written on the wire.
            pub fn as_str(&self) -> &str {
                match self {
                    $(Self::$variant => $value,)*
                    Self::Unknown(value) => value.as_str(),
                }
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                match value {
                    $($value => Self::$variant,)*
                    other => Self::Unknown(other.to_owned()),
                }
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                match value.as_str() {
                    $($value => Self::$variant,)*
                    _ => Self::Unknown(value),
                }
            }
        }

        impl ::std::str::FromStr for $name {
            type Err = ::std::convert::Infallible;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Ok(Self::from(value))
            }
        }

        impl ::serde::Serialize for $name {
            fn serialize<S: ::serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                if matches!(self, Self::Unknown(value) if value.is_empty()) {
                    return Err(<S::Error as ::serde::ser::Error>::custom(concat!(
                        "a value of `",
                        stringify!($name),
                        "` is not set (it still holds its default, the empty `Unknown`)"
                    )));
                }
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> ::serde::Deserialize<'de> for $name {
            fn deserialize<D: ::serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                <String as ::serde::Deserialize>::deserialize(deserializer).map(Self::from)
            }
        }

        impl $crate::request::QueryValue for $name {
            fn append_to(&self, name: &str, out: &mut Vec<(String, String)>) {
                out.push((name.to_owned(), self.as_str().to_owned()));
            }
        }
    };
}

/// Whether decoding failed only because a required property was absent
/// (at any depth). serde_json has no error category for it, so the message
/// that `serde::de::Error::missing_field` produces is recognised.
#[doc(hidden)]
pub fn is_missing_field(error: &serde_json::Error) -> bool {
    error.to_string().starts_with("missing field `")
}

/// Declares a union discriminated by a tag property. A value whose tag is
/// missing or not known to this version of the SDK is kept as raw JSON in
/// `Unknown`, so decoding never fails because of a new variant. The same
/// happens to a value that has a known tag but lacks a property its variant
/// requires: the models stay strict (nothing is filled with zeros), one
/// incomplete event does not break a stream or a webhook, and its JSON is
/// still there to look at. A property of the wrong JSON type is an error.
macro_rules! tagged_union {
    (
        $(#[$meta:meta])*
        $name:ident, tag = $tag:literal {
            $($(#[$vmeta:meta])* $variant:ident($inner:ty) = $value:literal,)*
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq)]
        #[non_exhaustive]
        #[allow(clippy::large_enum_variant)]
        pub enum $name {
            $($(#[$vmeta])* $variant($inner),)*
            /// A variant this version of the SDK does not know, or a value
            /// that lacks a property its variant requires, as raw JSON.
            Unknown(::serde_json::Value),
        }

        /// The default is `Unknown` holding JSON `null`; it stands for "not
        /// chosen yet" in a model built with `..Default::default()` and is
        /// never produced by decoding.
        impl Default for $name {
            fn default() -> Self {
                Self::Unknown(::serde_json::Value::Null)
            }
        }

        impl $name {
            /// The name of the JSON property that selects the variant.
            pub const TAG: &'static str = $tag;

            /// The value of the tag property, when the variant has one.
            pub fn tag(&self) -> Option<&str> {
                match self {
                    $(Self::$variant(_) => Some($value),)*
                    Self::Unknown(value) => value.get($tag).and_then(::serde_json::Value::as_str),
                }
            }
        }

        impl ::serde::Serialize for $name {
            fn serialize<S: ::serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                match self {
                    $(Self::$variant(inner) => ::serde::Serialize::serialize(inner, serializer),)*
                    Self::Unknown(value) => ::serde::Serialize::serialize(value, serializer),
                }
            }
        }

        impl<'de> ::serde::Deserialize<'de> for $name {
            fn deserialize<D: ::serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let value = <::serde_json::Value as ::serde::Deserialize>::deserialize(deserializer)?;
                let tag = value
                    .get($tag)
                    .and_then(::serde_json::Value::as_str)
                    .map(str::to_owned);
                match tag.as_deref() {
                    $(Some($value) => match <$inner as ::serde::Deserialize>::deserialize(&value) {
                        Ok(inner) => Ok(Self::$variant(inner)),
                        Err(error) if $crate::serde_util::is_missing_field(&error) => {
                            Ok(Self::Unknown(value))
                        }
                        Err(error) => Err(::serde::de::Error::custom(error)),
                    },)*
                    _ => Ok(Self::Unknown(value)),
                }
            }
        }
    };
}

pub(crate) use string_enum;
pub(crate) use tagged_union;
