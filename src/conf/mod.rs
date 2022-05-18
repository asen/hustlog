// Copyright 2022 Asen Lazarov

mod external;
mod args;
mod conf;

use std::error::Error;
use std::fmt;
pub use args::*;
pub use conf::*;

#[derive(Debug, Clone)]
pub struct ConfigError(String);

impl ConfigError {
    pub fn new(s: &str) -> ConfigError {
        ConfigError(s.to_string())
    }
}
impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Configuration error: {}", self.0)
    }
}

impl Error for ConfigError {}
