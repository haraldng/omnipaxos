/// Holds helpful functions used in creating loggers.
#[cfg(feature = "logging")]
pub mod logger;
/// Holds helpful functions used in OmniPaxosUI.
pub mod ui;

/// Holds `VecLike` type.
mod vec_like;
pub use vec_like::VecLike;
