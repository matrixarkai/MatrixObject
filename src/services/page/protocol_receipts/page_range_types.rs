
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageLocation {
    pub chunk_id: PageId,
    pub chunk_offset: u64,
    pub length: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageRangeWrite {
    pub slab_id: SlabId,
    pub offset: u64,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageRangeRequest {
    pub slab_id: SlabId,
    pub offset: u64,
    pub length: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageRangeReadResult {
    pub data: Vec<u8>,
    pub stats: PageReadStats,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PageRangeReadReport {
    pub data: Vec<u8>,
    pub stats: PageReadStats,
    pub payload_bytes: u64,
    pub max_payload_bytes: u64,
    pub max_chunk_read_count: u64,
    pub route_control_proof: PageRouteControlProof,
    pub expected_route_control_checksum: String,
    pub route_control_fenced: bool,
    pub route_control_fence_matched: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageAllocation {
    pub allocation_id: String,
    pub slab_id: SlabId,
    pub offset: u64,
    pub length: u64,
    pub create_time_ms: u64,
}

impl PageAllocation {
    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = self
            .slab_id
            .to_control_fields(&format!("{field_prefix}segment."));
        fields.insert(
            format!("{field_prefix}allocation_id"),
            self.allocation_id.clone(),
        );
        fields.insert(
            format!("{field_prefix}volume_id"),
            self.slab_id.volume_id.to_string(),
        );
        fields.insert(
            format!("{field_prefix}segment_id"),
            self.slab_id.id.to_string(),
        );
        fields.insert(format!("{field_prefix}offset"), self.offset.to_string());
        fields.insert(format!("{field_prefix}length"), self.length.to_string());
        fields.insert(
            format!("{field_prefix}create_time_ms"),
            self.create_time_ms.to_string(),
        );
        fields
    }
}

fn allocation_overlaps_chunk(
    allocation: &PageAllocation,
    page_index: u64,
    page_size: u64,
) -> bool {
    page_range_overlaps_chunk(
        allocation.offset,
        allocation.length,
        page_index,
        page_size,
    )
}

fn page_range_overlaps_chunk(offset: u64, length: u64, page_index: u64, page_size: u64) -> bool {
    if length == 0 || page_size == 0 {
        return false;
    }
    let Some(start) = page_index.checked_mul(page_size) else {
        return false;
    };
    let Some(end) = start.checked_add(page_size) else {
        return false;
    };
    let Some(range_end) = offset.checked_add(length) else {
        return false;
    };
    offset < end && range_end > start
}

