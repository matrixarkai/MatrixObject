// SPDX-License-Identifier: Apache-2.0
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkRouteQosConfig {
    pub window_ms: u64,
    pub max_read_ops_per_window: u64,
    pub max_read_bytes_per_window: u64,
    pub max_write_ops_per_window: u64,
    pub max_write_bytes_per_window: u64,
    pub max_discard_ops_per_window: u64,
    pub max_discard_bytes_per_window: u64,
}

impl Default for ChunkRouteQosConfig {
    fn default() -> Self {
        Self {
            window_ms: DEFAULT_PAGE_ROUTE_QOS_WINDOW_MS,
            max_read_ops_per_window: 0,
            max_read_bytes_per_window: 0,
            max_write_ops_per_window: 0,
            max_write_bytes_per_window: 0,
            max_discard_ops_per_window: 0,
            max_discard_bytes_per_window: 0,
        }
    }
}
