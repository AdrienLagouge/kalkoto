use crate::{
    entities::simulator::{
        SimulatorBuilder, ValidBaselineInput, ValidMenageInput, ValidVarianteInput,
    },
    KalkotoResult,
};

pub trait OutputAdapter {
    fn export_baseline_results<E>(
        &self,
        simulated: &SimulatorBuilder<ValidMenageInput, ValidBaselineInput, E>,
    ) -> KalkotoResult<()>;

    fn export_variante_results(
        &self,
        simulated: &SimulatorBuilder<ValidMenageInput, ValidBaselineInput, ValidVarianteInput>,
    ) -> KalkotoResult<()>;
}

pub mod csv_output_adapter;
