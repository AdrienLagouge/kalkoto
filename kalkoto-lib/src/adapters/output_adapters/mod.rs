use crate::{
    adapters::output_adapters::{
        arrow_output_adapter::ArrowOutputAdapter, csv_output_adapter::CSVOutputAdapter,
    },
    entities::simulator::{
        SimulatorBuilder, ValidBaselineInput, ValidMenageInput, ValidVarianteInput,
    },
    KalkotoResult,
};

pub trait OutputWriter {
    fn export_baseline_results<E>(
        &self,
        simulated: &SimulatorBuilder<ValidMenageInput, ValidBaselineInput, E>,
    ) -> KalkotoResult<()>;

    fn export_variante_results(
        &self,
        simulated: &SimulatorBuilder<ValidMenageInput, ValidBaselineInput, ValidVarianteInput>,
    ) -> KalkotoResult<()>;
}

pub mod arrow_output_adapter;
pub mod csv_output_adapter;

pub enum OutputAdapter {
    CSV(CSVOutputAdapter),
    Arrow(ArrowOutputAdapter),
}

impl OutputWriter for OutputAdapter {
    fn export_baseline_results<E>(
        &self,
        simulated: &SimulatorBuilder<ValidMenageInput, ValidBaselineInput, E>,
    ) -> KalkotoResult<()> {
        match self {
            Self::CSV(csv_output_adapter) => csv_output_adapter.export_baseline_results(simulated),
            Self::Arrow(arrow_output_adapter) => {
                arrow_output_adapter.export_baseline_results(simulated)
            }
        }
    }

    fn export_variante_results(
        &self,
        simulated: &SimulatorBuilder<ValidMenageInput, ValidBaselineInput, ValidVarianteInput>,
    ) -> KalkotoResult<()> {
        match self {
            Self::CSV(csv_output_adapter) => csv_output_adapter.export_variante_results(simulated),
            Self::Arrow(arrow_output_adapter) => {
                arrow_output_adapter.export_variante_results(simulated)
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum OutputAdapterError {
    #[error("Erreur à l'ouverture du fichier")]
    IO(#[from] std::io::Error),

    #[error("Erreur à l'écriture du fichier CSV")]
    CSV(#[from] csv::Error),

    #[error("Erreur à l'écriture du fichier Arrow")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("{0}")]
    Custom(String),
}
