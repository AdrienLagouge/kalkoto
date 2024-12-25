//! Crate prelude

pub use crate::errors::WorkflowError;

pub type Result<T> = core::result::Result<T, WorkflowError>;

//Wrapper générique pour implémenter le newtype pattern et contourner l'orphan rule
pub struct Wrapper<T>(pub T);
