//! Crate prelude

pub use crate::errors::KalkotoError;

pub type KalkotoResult<T> = core::result::Result<T, KalkotoError>;

//Wrapper générique pour implémenter le newtype pattern et contourner l'orphan rule
pub struct Wrapper<T>(pub T);
