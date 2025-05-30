//! Main Crate Error

#[derive(thiserror::Error, Debug)]
pub enum KalkotoError {
    #[error("Problème lors de la création de la liste des cas-types")]
    ListMenageError(#[from] crate::adapters::MenageListAdapterError),

    #[error("Problème lors de la création de la politique publique")]
    PolicyError(#[from] crate::adapters::PolicyAdapterError),

    #[error("Problème lors de la simulation")]
    SimError(#[from] crate::entities::simulator::SimulationError),

    #[error("Problème lors de l'exécution d'une fonction Python")]
    PythonError(#[from] pyo3::prelude::PyErr),

    #[error("Problème de synchronisation lors du calcul des résultats")]
    SyncError,
}

impl<T> From<std::sync::PoisonError<T>> for KalkotoError {
    fn from(err: std::sync::PoisonError<T>) -> Self {
        Self::SyncError
    }
}
