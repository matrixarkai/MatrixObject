// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
/// The user metadata or tags an object carries, held behind a pointer so an
/// object that carries none pays eight bytes rather than twenty-four.
///
/// Measured before this existed: the two `BTreeMap` handles were 48 of the 232
/// bytes of inline `ObjectMetadata`, and both held **zero bytes of content** in
/// every workload run. An empty `BTreeMap` allocates nothing, but its handle is
/// resident in every object and in every retained version of every object.
///
/// It derefs to the map, so reading one behaves exactly as the map does. A
/// mutable borrow materialises the map, because that is the only point at which
/// something could be inserted.
#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct ObjectAttributes(Option<Box<BTreeMap<String, String>>>);

impl ObjectAttributes {
    /// Whether the map behind the pointer has been allocated. The audit needs
    /// this: an object carrying no attributes owes nothing for them, and one
    /// that carries any owes the map's own inline size on the heap as well as
    /// its entries.
    pub(crate) fn is_allocated(&self) -> bool {
        self.0.is_some()
    }

    pub fn into_map(self) -> BTreeMap<String, String> {
        self.0.map(|map| *map).unwrap_or_default()
    }

    pub fn to_map(&self) -> BTreeMap<String, String> {
        self.0.as_deref().cloned().unwrap_or_default()
    }
}

impl std::ops::Deref for ObjectAttributes {
    type Target = BTreeMap<String, String>;

    fn deref(&self) -> &Self::Target {
        static EMPTY: std::sync::OnceLock<BTreeMap<String, String>> = std::sync::OnceLock::new();
        match self.0.as_deref() {
            Some(map) => map,
            None => EMPTY.get_or_init(BTreeMap::new),
        }
    }
}

impl std::ops::DerefMut for ObjectAttributes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.get_or_insert_with(Box::default)
    }
}

impl From<BTreeMap<String, String>> for ObjectAttributes {
    fn from(map: BTreeMap<String, String>) -> Self {
        if map.is_empty() {
            Self(None)
        } else {
            Self(Some(Box::new(map)))
        }
    }
}

impl From<ObjectAttributes> for BTreeMap<String, String> {
    fn from(attributes: ObjectAttributes) -> Self {
        attributes.into_map()
    }
}

impl FromIterator<(String, String)> for ObjectAttributes {
    fn from_iter<I: IntoIterator<Item = (String, String)>>(iter: I) -> Self {
        Self::from(iter.into_iter().collect::<BTreeMap<String, String>>())
    }
}

impl<'a> IntoIterator for &'a ObjectAttributes {
    type Item = (&'a String, &'a String);
    type IntoIter = std::collections::btree_map::Iter<'a, String, String>;

    fn into_iter(self) -> Self::IntoIter {
        (**self).iter()
    }
}
