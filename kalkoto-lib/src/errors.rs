//! Main Crate Error

#[derive(thiserror::Error, Debug)]
pub enum WorkflowError {
    #[error("Erreur générique: {0}")]
    Generic(String),

    #[error("Problème lors de la création de la liste des cas-types")]
    ListMenageError(#[from] crate::adapters::MenageListAdapterError),

    //    #[error("Problème lors de la création de la politique publique")]
    //   PolicyError(#[from] crate::adapters::PolicyAdapterError),
    #[error(transparent)]
    IO(#[from] std::io::Error),

    #[error("Erreur inconnue")]
    Unknown,
}