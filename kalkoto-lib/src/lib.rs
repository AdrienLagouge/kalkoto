#![allow(unused)]

pub mod adapters;
pub mod entities;
pub mod errors;

pub type KalkotoResult<T> = core::result::Result<T, crate::errors::KalkotoError>;

pub use crate::errors::KalkotoError;
