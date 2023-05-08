#![cfg(feature = "toml_config")]

use std::{error, fmt, io};
use toml;

/// Error type for the reading and parsing of OmniPaxosConfig TOML files
#[derive(Debug)]
pub enum ConfigError {
    /// Could not read TOML file
    ReadFile(io::Error),
    /// Could not parse TOML file
    Parse(toml::de::Error),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ConfigError::ReadFile(ref err) => write!(f, "Could not read config file: {}", err),
            ConfigError::Parse(ref err) => write!(f, "Could not parse config file:\n {}", err),
        }
    }
}

impl error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            ConfigError::ReadFile(ref err) => Some(err),
            ConfigError::Parse(ref err) => Some(err),
        }
    }
}

impl From<io::Error> for ConfigError {
    fn from(err: io::Error) -> ConfigError {
        ConfigError::ReadFile(err)
    }
}

impl From<toml::de::Error> for ConfigError {
    fn from(err: toml::de::Error) -> ConfigError {
        ConfigError::Parse(err)
    }
}
