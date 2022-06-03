use crate::DynError;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::BufReader;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ExternalConfig {
    pub input: Option<String>,
    pub merge_multi_line: Option<bool>,

    pub grok_schema_columns: Option<Vec<String>>,
    pub grok_pattern: Option<String>,
    pub grok_patterns_file: Option<String>,
    pub grok_extra_patterns: Option<Vec<String>>,
    pub grok_load_default: Option<bool>,
    pub grok_with_alias_only: Option<bool>,
    pub grok_ignore_default_patterns: Option<bool>,

    pub query: Option<String>,

    pub output: Option<String>,
    pub output_format: Option<String>,
    pub output_batch_size: Option<usize>,
    pub output_add_ddl: Option<bool>,

    pub rayon_threads: Option<usize>,
    pub tick_interval: Option<u64>,

    pub idle_timeout: Option<u64>,

    pub async_channel_size: Option<usize>,
    //pub async_file_processing: Option<bool>,
}

impl ExternalConfig {
    pub fn from_yaml_file(fname: &str) -> Result<ExternalConfig, DynError> {
        let rdr = BufReader::new(fs::File::open(fname)?);
        match serde_yaml::from_reader(rdr) {
            Ok(pc) => Ok(pc),
            Err(e) => Err(Box::new(e)),
        }
    }

    pub fn empty() -> Self {
        Self {
            input: None,
            merge_multi_line: None,
            grok_schema_columns: None,
            grok_pattern: None,
            grok_patterns_file: None,
            grok_extra_patterns: None,
            grok_load_default: None,
            grok_with_alias_only: None,
            grok_ignore_default_patterns: None,
            query: None,
            output: None,
            output_format: None,
            output_batch_size: None,
            output_add_ddl: None,
            rayon_threads: None,
            tick_interval: None,
            idle_timeout: None,
            async_channel_size: None,
            // async_file_processing: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::conf::external::ExternalConfig;
    use std::path::PathBuf;

    #[test]
    fn test_empty_deser() {
        let yaml = ":";
        let pc: ExternalConfig = serde_yaml::from_str(&yaml).unwrap();
        println!("{:?}", pc)
    }

    #[test]
    fn test_deser() {
        let yaml = "input: blah\n";
        let pc: ExternalConfig = serde_yaml::from_str(&yaml).unwrap();
        println!("{:?}", pc)
    }

    #[test]
    fn test_example_config() {
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("config_examples/syslog.yml");

        let pc = ExternalConfig::from_yaml_file(d.to_str().unwrap()).unwrap();
        println!("{:?}", pc)
    }
}
