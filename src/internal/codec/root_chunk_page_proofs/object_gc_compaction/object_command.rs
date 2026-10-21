// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
pub(crate) fn object_command_checksum(command: &ObjectCommand) -> String {
    etag(format!("{:?}", command).as_bytes())
}
