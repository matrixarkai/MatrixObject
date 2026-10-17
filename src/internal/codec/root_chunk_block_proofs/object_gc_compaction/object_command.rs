pub(crate) fn object_command_checksum(command: &ObjectCommand) -> String {
    etag(format!("{:?}", command).as_bytes())
}
