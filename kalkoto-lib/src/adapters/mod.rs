pub mod input_adapters;
pub mod output_adapters;

use input_adapters::{
    MenageListAdapter, MenageListAdapterError, PolicyAdapter, PolicyAdapterError,
};

pub use crate::{KalkotoError, KalkotoResult};
