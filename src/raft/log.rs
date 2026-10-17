pub trait RustRaftLog {
    fn append(&mut self, command: ObjectCommand) -> Result<RaftLogEntry, ObjectError>;
    fn commit_index(&self) -> u64;
    fn term(&self) -> u64;
    fn entries_from(&self, index: u64) -> Vec<RaftLogEntry>;
}

#[derive(Clone, Debug)]
pub struct InMemoryRustRaft {
    pub(crate) term: u64,
    pub(crate) commit_index: u64,
    pub(crate) entries: Vec<RaftLogEntry>,
}

impl InMemoryRustRaft {
    pub fn new(term: u64) -> Result<Self, ObjectError> {
        if term == 0 {
            return Err(ObjectError::InvalidArgument(
                "raft term must be positive".to_string(),
            ));
        }
        Ok(Self {
            term,
            commit_index: 0,
            entries: Vec::new(),
        })
    }
}

impl RustRaftLog for InMemoryRustRaft {
    fn append(&mut self, command: ObjectCommand) -> Result<RaftLogEntry, ObjectError> {
        let index = self
            .commit_index
            .checked_add(1)
            .ok_or_else(|| ObjectError::InvalidArgument("raft index overflow".to_string()))?;
        let entry = RaftLogEntry {
            term: self.term,
            index,
            command,
        };
        self.entries.push(entry.clone());
        self.commit_index = index;
        Ok(entry)
    }

    fn commit_index(&self) -> u64 {
        self.commit_index
    }

    fn term(&self) -> u64 {
        self.term
    }

    fn entries_from(&self, index: u64) -> Vec<RaftLogEntry> {
        self.entries
            .iter()
            .filter(|entry| entry.index >= index)
            .cloned()
            .collect()
    }
}
