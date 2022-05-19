#[derive(Debug)]
pub struct SyslogServerConfig {
    pub proto: String,
    // TODO
    pub listen_host: String,
    pub port: u32,
}
