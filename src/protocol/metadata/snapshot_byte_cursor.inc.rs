// SPDX-License-Identifier: Apache-2.0
pub(crate) struct SnapshotByteCursor<'a> {
    data: &'a [u8],
    cursor: usize,
}

impl<'a> SnapshotByteCursor<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self { data, cursor: 0 }
    }

    pub(crate) fn position(&self) -> usize {
        self.cursor
    }

    pub(crate) fn read_u64(&mut self) -> Result<u64, ObjectError> {
        let end = self.cursor.checked_add(8).ok_or_else(|| {
            ObjectError::Corruption("metadata snapshot cursor overflow".to_string())
        })?;
        if end > self.data.len() {
            return Err(ObjectError::Corruption(
                "metadata snapshot integer is truncated".to_string(),
            ));
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&self.data[self.cursor..end]);
        self.cursor = end;
        Ok(u64::from_le_bytes(bytes))
    }

    pub(crate) fn read_bool(&mut self) -> Result<bool, ObjectError> {
        match self.read_u64()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(ObjectError::Corruption(
                "metadata snapshot boolean is invalid".to_string(),
            )),
        }
    }

    pub(crate) fn read_count(&mut self, label: &str) -> Result<usize, ObjectError> {
        let count = self.read_u64()?;
        if count > 1_000_000 {
            return Err(ObjectError::Corruption(format!("{} is too large", label)));
        }
        Ok(count as usize)
    }

    pub(crate) fn read_string(&mut self) -> Result<String, ObjectError> {
        let len = self.read_u64()?;
        if len > 64 * 1024 * 1024 {
            return Err(ObjectError::Corruption(
                "metadata snapshot string is too large".to_string(),
            ));
        }
        let len = len as usize;
        let end = self.cursor.checked_add(len).ok_or_else(|| {
            ObjectError::Corruption("metadata snapshot cursor overflow".to_string())
        })?;
        if end > self.data.len() {
            return Err(ObjectError::Corruption(
                "metadata snapshot string is truncated".to_string(),
            ));
        }
        let value = std::str::from_utf8(&self.data[self.cursor..end])
            .map_err(|_| {
                ObjectError::Corruption("metadata snapshot string is not utf8".to_string())
            })?
            .to_string();
        self.cursor = end;
        Ok(value)
    }

    pub(crate) fn read_bytes(&mut self, len: usize) -> Result<&'a [u8], ObjectError> {
        let end = self
            .cursor
            .checked_add(len)
            .ok_or_else(|| ObjectError::Corruption("snapshot byte cursor overflow".to_string()))?;
        if end > self.data.len() {
            return Err(ObjectError::Corruption(
                "snapshot byte payload is truncated".to_string(),
            ));
        }
        let out = &self.data[self.cursor..end];
        self.cursor = end;
        Ok(out)
    }
}
