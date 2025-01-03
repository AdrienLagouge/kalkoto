//! Main Crate Error

#[derive(thiserror::Error, Debug)]
pub enum KalkotoError {
    #[error("Problème lors de la création de la liste des cas-types")]
    ListMenageError(#[from] crate::adapters::MenageListAdapterError),

    #[error("Problème lors de la création de la politique publique")]
    PolicyError(#[from] crate::adapters::PolicyAdapterError),

    #[error("Erreur inconnue")]
    Unknown,
}
