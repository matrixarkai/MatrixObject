// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
/// Text that several holders can name the same object with, without each
/// keeping its own copy of the characters.
///
/// Measured before this existed: six resident copies of every key character,
/// 6.0 bytes per character per object. Four of the six are a container and the
/// value inside it naming the same object, and a clone of this type is a
/// refcount bump rather than a copy of the characters, so those collapse to one
/// allocation as long as the holders are clones of one value rather than
/// independently built.
///
/// It derefs to `str`, so a borrow of it behaves as the characters do; what it
/// does not do is convert silently to `String`, because an accidental
/// `to_string()` is exactly how the sharing is lost.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub struct SharedText(std::sync::Arc<str>);

impl SharedText {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether two holders name the object through the same allocation. This
    /// is the only honest test that sharing happened: a byte figure can move
    /// for other reasons, and a text comparison cannot tell a share from a
    /// copy.
    pub fn shares_with(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.0, &other.0)
    }

    /// Which allocation the characters live in. Used by the footprint audit to
    /// charge one allocation once however many holders name it; it is an
    /// address, so it is meaningful only within one audit pass.
    pub(crate) fn allocation_id(&self) -> usize {
        std::sync::Arc::as_ptr(&self.0) as *const u8 as usize
    }
}

impl std::ops::Deref for SharedText {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl std::convert::AsRef<str> for SharedText {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::borrow::Borrow<str> for SharedText {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SharedText {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl From<String> for SharedText {
    fn from(value: String) -> Self {
        Self(value.into())
    }
}

impl From<&String> for SharedText {
    fn from(value: &String) -> Self {
        Self(value.as_str().into())
    }
}

impl From<&str> for SharedText {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

impl From<std::sync::Arc<str>> for SharedText {
    fn from(value: std::sync::Arc<str>) -> Self {
        Self(value)
    }
}

impl From<SharedText> for String {
    fn from(value: SharedText) -> Self {
        value.0.to_string()
    }
}

impl From<&SharedText> for String {
    fn from(value: &SharedText) -> Self {
        value.0.to_string()
    }
}

impl PartialEq<str> for SharedText {
    fn eq(&self, other: &str) -> bool {
        &*self.0 == other
    }
}

impl PartialEq<&str> for SharedText {
    fn eq(&self, other: &&str) -> bool {
        &*self.0 == *other
    }
}

impl PartialEq<String> for SharedText {
    fn eq(&self, other: &String) -> bool {
        &*self.0 == other.as_str()
    }
}

impl PartialEq<SharedText> for str {
    fn eq(&self, other: &SharedText) -> bool {
        self == &*other.0
    }
}

impl PartialEq<SharedText> for String {
    fn eq(&self, other: &SharedText) -> bool {
        self.as_str() == &*other.0
    }
}
