use crate::{
    adapters::output_adapters::OutputAdapter,
    entities::simulator::{
        SimulationError, SimulatorBuilder, ValidBaselineInput, ValidMenageInput, ValidVarianteInput,
    },
    KalkotoError, KalkotoResult,
};
use csv::WriterBuilder;

#[derive(Default)]
pub struct CSVOutputAdapter {
    output_prefix: Option<String>,
}

impl CSVOutputAdapter {
    pub fn new() -> Self {
        CSVOutputAdapter::default()
    }

    pub fn add_output_prefix(&mut self, prefix: String) -> Self {
        Self {
            output_prefix: Some(prefix.clone()),
        }
    }
}

impl OutputAdapter for CSVOutputAdapter {
    fn export_baseline_results<E>(
        &self,
        simulated: &SimulatorBuilder<ValidMenageInput, ValidBaselineInput, E>,
    ) -> KalkotoResult<()> {
        let output_path = match &self.output_prefix {
            Some(output_prefix) => format!("{}-baseline-results.csv", output_prefix),
            _ => String::from("baseline-results.csv"),
        };

        if let Some(baseline_results) = &simulated.results_baseline {
            let mut wtr = WriterBuilder::new()
                .delimiter(b';')
                .from_path(output_path)
                .map_err(SimulationError::from)?;

            let mut headers = simulated
                .policy_baseline
                .0
                .valid_policy
                .composantes_ordonnees
                .iter()
                .map(|composante| composante.name.clone())
                .collect::<Vec<String>>();

            headers.insert(0, "Index".to_string());

            wtr.write_record(&headers);

            headers.remove(0);

            for (index, results_menage) in baseline_results.iter().enumerate() {
                let mut vec_results_menage = vec![];
                for name in headers.iter() {
                    vec_results_menage.push(
                        results_menage
                            .get(name)
                            .ok_or_else(|| {
                                SimulationError::from(
                                    format!("Problème de cohérence des composantes lors de l'export. Erreur : {}",name)
                                )
                            })?
                            .to_string(),
                    );
                }
                vec_results_menage.insert(0, (index + 1).to_string());
                wtr.write_record(&vec_results_menage);
            }

            wtr.flush().map_err(SimulationError::from)?;
            return Ok(());
        }

        Err(KalkotoError::SimError(SimulationError::from(
            "Pas possible d'exporter : les résultats n'ont pas encore été calculés".to_string(),
        )))
    }

    fn export_variante_results(
        &self,
        simulated: &SimulatorBuilder<ValidMenageInput, ValidBaselineInput, ValidVarianteInput>,
    ) -> KalkotoResult<()> {
        let (output_path_var, output_path_diff) = match &self.output_prefix {
            Some(output_prefix) => (
                format!("{}-variante-results.csv", output_prefix),
                format!("{}-diff-results.csv", output_prefix),
            ),
            _ => (
                String::from("baseline-results.csv"),
                String::from("diff-results.csv"),
            ),
        };

        if let (Some(variante_results), Some(diff_results)) =
            (&simulated.results_variante, &simulated.results_diff)
        {
            let mut wtr_var = WriterBuilder::new()
                .delimiter(b';')
                .from_path(output_path_var)
                .map_err(SimulationError::from)?;

            let mut wtr_diff = WriterBuilder::new()
                .delimiter(b';')
                .from_path(output_path_diff)
                .map_err(SimulationError::from)?;

            let mut headers = simulated
                .policy_variante
                .0
                .valid_policy
                .composantes_ordonnees
                .iter()
                .map(|composante| composante.name.clone())
                .collect::<Vec<String>>();

            headers.insert(0, "Index".to_string());

            wtr_var.write_record(&headers);
            wtr_diff.write_record(&headers);

            headers.remove(0);

            for (index, (results_menage_variante, results_menage_diff)) in
                variante_results.iter().zip(diff_results.iter()).enumerate()
            {
                let mut vec_results_menage_variante = vec![];
                let mut vec_results_menage_diff = vec![];
                for name in headers.iter() {
                    vec_results_menage_variante.push(
                        results_menage_variante
                            .get(name)
                            .ok_or(SimulationError::from(
                                "Problème de cohérence des composantes lors de l'export"
                                    .to_string(),
                            ))?
                            .to_string(),
                    );
                    vec_results_menage_diff.push(
                        results_menage_diff
                            .get(name)
                            .ok_or(SimulationError::from(
                                "Problème de cohérence des composantes lors de l'export"
                                    .to_string(),
                            ))?
                            .to_string(),
                    );
                }
                vec_results_menage_variante.insert(0, (index + 1).to_string());
                vec_results_menage_diff.insert(0, (index + 1).to_string());
                wtr_var.write_record(&vec_results_menage_variante);
                wtr_diff.write_record(&vec_results_menage_diff);
            }

            wtr_var.flush().map_err(SimulationError::from)?;
            wtr_diff.flush().map_err(SimulationError::from)?;
            return Ok(());
        }

        Err(KalkotoError::SimError(SimulationError::from(
            "Pas possible d'exporter : les résultats n'ont pas encore été calculés".to_string(),
        )))
    }
}
