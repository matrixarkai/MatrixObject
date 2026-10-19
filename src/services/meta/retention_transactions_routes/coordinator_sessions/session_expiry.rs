// SPDX-License-Identifier: Apache-2.0
impl MatrixMetaService {
    pub fn expire_coordinator_sessions(
        &mut self,
        now_ms: u64,
        limit: usize,
    ) -> Vec<CoordinatorSession> {
        self.expire_coordinator_sessions_including_fenced(now_ms, limit, false)
    }

    pub fn expire_coordinator_sessions_including_fenced(
        &mut self,
        now_ms: u64,
        limit: usize,
        include_fenced: bool,
    ) -> Vec<CoordinatorSession> {
        let take = if limit == 0 { usize::MAX } else { limit };
        let expired_ids: Vec<String> = self
            .coordinator_sessions
            .iter()
            .filter(|(_, session)| {
                self.coordinator_session_expirable(session, now_ms, include_fenced)
            })
            .map(|(session_id, _)| session_id.clone())
            .take(take)
            .collect();
        let mut expired = Vec::with_capacity(expired_ids.len());
        for session_id in expired_ids {
            if let Some(session) = self.coordinator_sessions.remove(&session_id) {
                expired.push(session);
            }
        }
        if !expired.is_empty() {
            self.next_commit_index = self.next_commit_index.saturating_add(1);
        }
        expired
    }

    fn plan_expired_coordinator_sessions(
        &self,
        now_ms: u64,
        limit: usize,
    ) -> Vec<CoordinatorSession> {
        self.plan_expired_coordinator_sessions_including_fenced(now_ms, limit, false)
    }

    fn plan_expired_coordinator_sessions_including_fenced(
        &self,
        now_ms: u64,
        limit: usize,
        include_fenced: bool,
    ) -> Vec<CoordinatorSession> {
        let take = if limit == 0 { usize::MAX } else { limit };
        self.coordinator_sessions
            .iter()
            .filter(|(_, session)| {
                self.coordinator_session_expirable(session, now_ms, include_fenced)
            })
            .map(|(_, session)| session.clone())
            .take(take)
            .collect()
    }

    fn coordinator_session_expirable(
        &self,
        session: &CoordinatorSession,
        now_ms: u64,
        include_fenced: bool,
    ) -> bool {
        session.expire_time_ms <= now_ms
            || (include_fenced && !self.coordinator_session_endpoint_is_fenced(&session.endpoint))
    }
}
