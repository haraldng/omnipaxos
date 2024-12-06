#[cfg(feature = "toml_config")]
use std::io;
use std::{error, fmt};
#[cfg(feature = "toml_config")]
use toml;

/// Error type for the reading and parsing of OmniPaxosConfig TOML files
#[derive(Debug)]
pub enum ConfigError {
    #[cfg(feature = "toml_config")]
    /// Could not read TOML file
    ReadFile(io::Error),
    #[cfg(feature = "toml_config")]
    /// Could not parse TOML file
    Parse(toml::de::Error),
    /// Invalid config fields
    InvalidConfig(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            #[cfg(feature = "toml_config")]
            ConfigError::ReadFile(err) => write!(f, "{}", err),
            #[cfg(feature = "toml_config")]
            ConfigError::Parse(err) => write!(f, "{}", err),
            ConfigError::InvalidConfig(str) => write!(f, "Invalid config: {}", str),
        }
    }
}

impl error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            #[cfg(feature = "toml_config")]
            ConfigError::ReadFile(err) => Some(err),
            #[cfg(feature = "toml_config")]
            ConfigError::Parse(err) => Some(err),
            ConfigError::InvalidConfig(_) => None,
        }
    }
}

#[cfg(feature = "toml_config")]
impl From<io::Error> for ConfigError {
    fn from(err: io::Error) -> ConfigError {
        ConfigError::ReadFile(err)
    }
}

#[cfg(feature = "toml_config")]
impl From<toml::de::Error> for ConfigError {
    fn from(err: toml::de::Error) -> ConfigError {
        ConfigError::Parse(err)
    }
}

#[allow(missing_docs)]
macro_rules! valid_config {
    ($pred:expr,$err_str:expr) => {
        if !$pred {
            return Err(ConfigError::InvalidConfig($err_str.to_owned()));
        }
    };
}
pub(crate) use valid_config;
