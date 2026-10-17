impl MatrixObjectStore {
    fn apply_root_placement_metadata_command(&mut self, command: ObjectCommand) -> Result<ObjectMetadata, ObjectError> {
        match command {
            ObjectCommand::RepairDrainingRootPlacements { max_objects } => {
                let report = self.repair_draining_root_placements(max_objects);
                Ok(metadata_control_metadata(
                    "_rootrepair",
                    "draining",
                    report.repair_objects,
                    report.bytes_repaired,
                    "root/placement-draining-repair",
                    now_millis(),
                ))
            }
            ObjectCommand::RepairUnhealthyRootPlacements {
                marker,
                max_objects,
                expected_replica_count,
                min_free_bytes,
                max_payload_bytes,
            } => {
                let report = self.repair_unhealthy_root_placements(
                    marker.as_ref(),
                    max_objects,
                    expected_replica_count,
                    min_free_bytes,
                    max_payload_bytes,
                )?;
                Ok(metadata_control_metadata(
                    "_rootrepair",
                    "health",
                    report.repair_objects,
                    report.bytes_repaired,
                    if report.has_more {
                        "root/placement-health-repair-more"
                    } else {
                        "root/placement-health-repair"
                    },
                    report.repaired_objects,
                ))
            }
            ObjectCommand::RebalanceRootCapacity {
                max_objects,
                min_free_bytes,
            } => {
                let report = self.rebalance_root_capacity(max_objects, min_free_bytes);
                Ok(metadata_control_metadata(
                    "_rootrebalance",
                    "capacity",
                    report.rebalance_objects,
                    report.bytes_rebalanced,
                    "root/placement-capacity-rebalance",
                    now_millis(),
                ))
            }
            ObjectCommand::ApplyPreparedRootPlacementMigration { prepared } => {
                let placement = self.apply_prepared_root_placement_migration(prepared)?;
                Ok(metadata_control_metadata(
                    "_rootmigration",
                    &placement.object_id.bucket,
                    placement.version,
                    placement.length,
                    "root/prepared-placement-migration",
                    self.last_applied_index(),
                ))
            }
            ObjectCommand::ApplyPreparedRootPlacementRepair { prepared } => {
                let report = self.apply_prepared_draining_root_placement_repair(prepared)?;
                Ok(metadata_control_metadata(
                    "_rootrepair",
                    "prepared-draining",
                    report.repaired_objects,
                    report.repair_objects,
                    if report.has_more {
                        "root/prepared-placement-draining-repair-more"
                    } else {
                        "root/prepared-placement-draining-repair"
                    },
                    report.bytes_repaired,
                ))
            }
            ObjectCommand::ApplyPreparedRootUnhealthyPlacementRepair { prepared } => {
                let report = self.apply_prepared_unhealthy_root_placement_repair(prepared)?;
                Ok(metadata_control_metadata(
                    "_rootrepair",
                    "prepared-health",
                    report.repaired_objects,
                    report.repair_objects,
                    if report.has_more {
                        "root/prepared-placement-health-repair-more"
                    } else {
                        "root/prepared-placement-health-repair"
                    },
                    report.bytes_repaired,
                ))
            }
            ObjectCommand::ApplyPreparedRootCapacityRebalance { prepared } => {
                let report = self.apply_prepared_root_capacity_rebalance(prepared)?;
                Ok(metadata_control_metadata(
                    "_rootrebalance",
                    "prepared",
                    report.rebalanced_objects,
                    report.rebalance_objects,
                    "root/prepared-placement-capacity-rebalance",
                    report.bytes_rebalanced,
                ))
            }
            _ => unreachable!("command classified outside RootPlacement"),
        }
    }
}
