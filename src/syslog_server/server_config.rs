#[derive(Debug)]
pub struct SyslogServerConfig {
    pub proto: String,
    // TODO
    pub listen_host: String,
    pub port: u32,
}

impl SyslogServerConfig {
    pub fn get_host_port(&self) -> String {
        format!("{}:{}", &self.listen_host, &self.port)
    }
}
