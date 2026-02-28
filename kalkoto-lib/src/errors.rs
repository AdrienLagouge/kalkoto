//! Main Crate Error

#[derive(thiserror::Error, Debug)]
pub enum KalkotoError {
    #[error("Problème lors de la création de la liste des cas-types")]
    ListMenageError(#[from] crate::adapters::input_adapters::MenageListAdapterError),

    #[error("Problème lors de la création de la politique publique")]
    PolicyError(#[from] crate::adapters::input_adapters::PolicyAdapterError),

    #[error("Problème lors de la simulation")]
    SimError(#[from] crate::entities::simulator::SimulationError),

    #[error("Problème à l'export des résultats")]
    ExportError(#[from] crate::adapters::output_adapters::OutputAdapterError),
}

pub type KalkotoResult<T> = core::result::Result<T, crate::errors::KalkotoError>;
