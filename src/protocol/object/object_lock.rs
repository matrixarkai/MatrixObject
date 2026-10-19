// SPDX-License-Identifier: Apache-2.0
/// What an object under restore, legal hold or retention carries.
///
/// Three timestamps and a flag, which are zero and false for every object that
/// has never been restored, held or placed under retention -- which is almost
/// all of them. Twenty-five inline bytes in every resident `ObjectMetadata` and
/// in every retained version of one.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectLockState {
    pub restore_until_ms: u64,
    pub legal_hold: bool,
    pub legal_hold_update_time_ms: u64,
    pub retention_until_ms: u64,
}

/// `ObjectLockState` behind a pointer, so an object under none of the three
/// pays eight bytes rather than twenty-five.
///
/// It derefs to the state, so reading a field behaves as it did when the fields
/// were inline. A mutable borrow materialises the state, that being the only
/// point at which one of them could become non-zero.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObjectLock(Option<Box<ObjectLockState>>);

impl ObjectLock {
    /// Whether the state behind the pointer has been allocated. The audit needs
    /// this: an object under none of the three owes nothing for them.
    pub(crate) fn is_allocated(&self) -> bool {
        self.0.is_some()
    }
}

impl std::ops::Deref for ObjectLock {
    type Target = ObjectLockState;

    fn deref(&self) -> &Self::Target {
        static NONE: std::sync::OnceLock<ObjectLockState> = std::sync::OnceLock::new();
        match self.0.as_deref() {
            Some(state) => state,
            None => NONE.get_or_init(ObjectLockState::default),
        }
    }
}

impl std::ops::DerefMut for ObjectLock {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.get_or_insert_with(Box::default)
    }
}

impl From<ObjectLockState> for ObjectLock {
    fn from(state: ObjectLockState) -> Self {
        if state == ObjectLockState::default() {
            Self(None)
        } else {
            Self(Some(Box::new(state)))
        }
    }
}

impl ObjectLock {
    /// Build one from the four values a decode or a transition has in hand,
    /// collapsing the all-default case to no allocation.
    pub fn new(
        restore_until_ms: u64,
        legal_hold: bool,
        legal_hold_update_time_ms: u64,
        retention_until_ms: u64,
    ) -> Self {
        Self::from(ObjectLockState {
            restore_until_ms,
            legal_hold,
            legal_hold_update_time_ms,
            retention_until_ms,
        })
    }
}
