// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
impl MatrixReplicaService {
    pub fn configure_trim_flow(
        &mut self,
        rate_bytes_per_sec: u64,
        burst_bytes: u64,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        if (rate_bytes_per_sec == 0) != (burst_bytes == 0) {
            return Err(ObjectError::InvalidArgument(
                "trim flow rate and burst must both be zero or both be positive".to_string(),
            ));
        }
        self.refill_trim_flow(now_ms);
        self.trim_flow_rate_bytes_per_sec = rate_bytes_per_sec;
        self.trim_flow_burst_bytes = burst_bytes;
        self.trim_flow_last_refill_ms = now_ms;
        if rate_bytes_per_sec == 0 {
            self.trim_flow_available_bytes = 0;
        } else if self.trim_flow_available_bytes == 0 {
            self.trim_flow_available_bytes = burst_bytes;
        } else {
            self.trim_flow_available_bytes = self.trim_flow_available_bytes.min(burst_bytes);
        }
        Ok(())
    }

    pub fn configure_trim_flow_ops(
        &mut self,
        rate_ops_per_sec: u64,
        burst_ops: u64,
        now_ms: u64,
    ) -> Result<(), ObjectError> {
        if (rate_ops_per_sec == 0) != (burst_ops == 0) {
            return Err(ObjectError::InvalidArgument(
                "trim flow op rate and burst must both be zero or both be positive".to_string(),
            ));
        }
        self.refill_trim_flow(now_ms);
        self.trim_flow_rate_ops_per_sec = rate_ops_per_sec;
        self.trim_flow_burst_ops = burst_ops;
        self.trim_flow_last_refill_ms = now_ms;
        if rate_ops_per_sec == 0 {
            self.trim_flow_available_ops = 0;
        } else if self.trim_flow_available_ops == 0 {
            self.trim_flow_available_ops = burst_ops;
        } else {
            self.trim_flow_available_ops = self.trim_flow_available_ops.min(burst_ops);
        }
        Ok(())
    }

    pub fn trim_flow_rate_bytes_per_sec(&self) -> u64 {
        self.trim_flow_rate_bytes_per_sec
    }

    pub fn trim_flow_burst_bytes(&self) -> u64 {
        self.trim_flow_burst_bytes
    }

    pub fn trim_flow_available_bytes(&self) -> u64 {
        self.trim_flow_available_bytes
    }

    pub fn trim_flow_rate_ops_per_sec(&self) -> u64 {
        self.trim_flow_rate_ops_per_sec
    }

    pub fn trim_flow_burst_ops(&self) -> u64 {
        self.trim_flow_burst_ops
    }

    pub fn trim_flow_available_ops(&self) -> u64 {
        self.trim_flow_available_ops
    }

    pub fn trim_flow_throttled_reclaims(&self) -> u64 {
        self.trim_flow_throttled_reclaims
    }

    pub fn trim_flow_throttled_ops(&self) -> u64 {
        self.trim_flow_throttled_ops
    }

    pub fn trim_flow_throttled_bytes(&self) -> u64 {
        self.trim_flow_throttled_bytes
    }

    fn estimated_trim_flow_available_bytes(&self, now_ms: u64) -> u64 {
        if !self.trim_flow_bytes_enabled() {
            return 0;
        }
        if now_ms <= self.trim_flow_last_refill_ms {
            return self
                .trim_flow_available_bytes
                .min(self.trim_flow_burst_bytes);
        }
        let elapsed_ms = now_ms.saturating_sub(self.trim_flow_last_refill_ms) as u128;
        let refill = elapsed_ms
            .saturating_mul(self.trim_flow_rate_bytes_per_sec as u128)
            .saturating_div(1_000)
            .min(u64::MAX as u128) as u64;
        self.trim_flow_available_bytes
            .saturating_add(refill)
            .min(self.trim_flow_burst_bytes)
    }

    fn estimated_trim_flow_available_ops(&self, now_ms: u64) -> u64 {
        if !self.trim_flow_ops_enabled() {
            return 0;
        }
        if now_ms <= self.trim_flow_last_refill_ms {
            return self.trim_flow_available_ops.min(self.trim_flow_burst_ops);
        }
        let elapsed_ms = now_ms.saturating_sub(self.trim_flow_last_refill_ms) as u128;
        let refill = elapsed_ms
            .saturating_mul(self.trim_flow_rate_ops_per_sec as u128)
            .saturating_div(1_000)
            .min(u64::MAX as u128) as u64;
        self.trim_flow_available_ops
            .saturating_add(refill)
            .min(self.trim_flow_burst_ops)
    }

    fn trim_flow_bytes_enabled(&self) -> bool {
        self.trim_flow_rate_bytes_per_sec > 0 && self.trim_flow_burst_bytes > 0
    }

    fn trim_flow_ops_enabled(&self) -> bool {
        self.trim_flow_rate_ops_per_sec > 0 && self.trim_flow_burst_ops > 0
    }

    fn trim_flow_enabled(&self) -> bool {
        self.trim_flow_bytes_enabled() || self.trim_flow_ops_enabled()
    }

    fn refill_trim_flow(&mut self, now_ms: u64) {
        if !self.trim_flow_enabled() {
            self.trim_flow_last_refill_ms = now_ms;
            return;
        }
        if now_ms <= self.trim_flow_last_refill_ms {
            return;
        }
        let elapsed_ms = now_ms.saturating_sub(self.trim_flow_last_refill_ms) as u128;
        if self.trim_flow_bytes_enabled() {
            let refill = elapsed_ms
                .saturating_mul(self.trim_flow_rate_bytes_per_sec as u128)
                .saturating_div(1_000)
                .min(u64::MAX as u128) as u64;
            self.trim_flow_available_bytes = self
                .trim_flow_available_bytes
                .saturating_add(refill)
                .min(self.trim_flow_burst_bytes);
        }
        if self.trim_flow_ops_enabled() {
            let refill = elapsed_ms
                .saturating_mul(self.trim_flow_rate_ops_per_sec as u128)
                .saturating_div(1_000)
                .min(u64::MAX as u128) as u64;
            self.trim_flow_available_ops = self
                .trim_flow_available_ops
                .saturating_add(refill)
                .min(self.trim_flow_burst_ops);
        }
        self.trim_flow_last_refill_ms = now_ms;
    }

    fn effective_trim_budget(&mut self, requested_trim_bytes: u64, now_ms: u64) -> (u64, u64, u64) {
        if !self.trim_flow_bytes_enabled() {
            return (requested_trim_bytes, 0, 0);
        }
        self.refill_trim_flow(now_ms);
        let effective = requested_trim_bytes.min(self.trim_flow_available_bytes);
        let throttled = requested_trim_bytes.saturating_sub(effective);
        (effective, self.trim_flow_available_bytes, throttled)
    }

}
