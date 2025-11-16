use std::{collections::HashMap, sync::Arc, thread};

use crate::{
    adapters::output_adapters::{OutputAdapterError, OutputWriter},
    entities::simulator::{
        SimulationError, SimulatorBuilder, ValidBaselineInput, ValidMenageInput, ValidVarianteInput,
    },
    KalkotoError, KalkotoResult,
};
use csv::{Result, WriterBuilder};

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

    fn export_variante_results(
        &self,
        simulated: Arc<SimulatorBuilder<ValidMenageInput, ValidBaselineInput, ValidVarianteInput>>,
    ) -> KalkotoResult<()> {
        let output_path_var = match &self.output_prefix {
            Some(output_prefix) => format!("{}-variante-results.csv", output_prefix),
            _ => String::from("variante-results.csv"),
        };

        if let Some(variante_results) = &simulated.results_variante {
            let text_results_dict: Vec<HashMap<&str, String>> = variante_results
                .iter()
                .map(|menage_results| {
                    menage_results
                        .iter()
                        .map(|(k, v)| (k.as_str(), format!("{}", v)))
                        .collect()
                })
                .collect();

            let mut text_caract_dict: Vec<HashMap<&str, String>> = simulated
                .menage_input
                .0
                .liste_menage_valide
                .iter()
                .map(|menage| {
                    menage
                        .caracteristiques
                        .iter()
                        .map(|(k, v)| (k.as_str(), format!("{}", v)))
                        .collect()
                })
                .collect();

            let joined_menages_variante_results: Vec<HashMap<&str, String>> = text_caract_dict
                .into_iter()
                .zip(text_results_dict)
                .map(|(mut caract_dict, results_dict)| {
                    caract_dict.extend(results_dict);
                    caract_dict
                })
                .collect();

            let mut wtr_var = WriterBuilder::new()
                .delimiter(b';')
                .from_path(output_path_var)
                .map_err(OutputAdapterError::from)?;

            let mut headers = simulated
                .policy_variante
                .0
                .valid_policy
                .composantes_ordonnees
                .iter()
                .map(|composante| composante.name.as_str())
                .chain(
                    simulated
                        .menage_input
                        .0
                        .set_caracteristiques_valide
                        .iter()
                        .map(|caracteristique_name| caracteristique_name.as_str()),
                )
                .collect::<Vec<&str>>();

            headers.sort_unstable();

            headers.insert(0, "Index");

            wtr_var.write_record(&headers);

            headers.remove(0);

            for (index, results_menage_variante) in
                joined_menages_variante_results.into_iter().enumerate()
            {
                let mut vec_results_menage_variante = vec![];
                for name in headers.iter() {
                    let out_variante_result = results_menage_variante
                        .get(name)
                        .ok_or(SimulationError::ResultsError(format!(
                            "Problème de cohérence des composantes lors de l'export. Erreur à la composante : {}",
                            name
                        )))?
                        ;

                    vec_results_menage_variante.push(out_variante_result);
                }

                let index_mod = (index + 1).to_string();
                vec_results_menage_variante.insert(0, &index_mod);

                wtr_var.write_record(&vec_results_menage_variante);
            }

            wtr_var.flush().map_err(OutputAdapterError::from)?;
            return Ok(());
        }

        Err(KalkotoError::SimError(SimulationError::ResultsError(
            "Pas possible d'exporter : les résultats n'ont pas encore été calculés".to_string(),
        )))
    }

    fn export_diff_results(
        &self,
        simulated: Arc<SimulatorBuilder<ValidMenageInput, ValidBaselineInput, ValidVarianteInput>>,
    ) -> KalkotoResult<()> {
        let output_path_diff = match &self.output_prefix {
            Some(output_prefix) => format!("{}-diff-results.csv", output_prefix),
            _ => String::from("diff-results.csv"),
        };

        if let Some(diff_results) = &simulated.results_diff {
            let text_results_dict: Vec<HashMap<&str, String>> = diff_results
                .iter()
                .map(|menage_results| {
                    menage_results
                        .iter()
                        .map(|(k, v)| match (k, v) {
                            (k, Some(v)) => (k.as_str(), format!("{}", v)),
                            (k, None) => (k.as_str(), String::default()),
                        })
                        .collect()
                })
                .collect();

            let mut text_caract_dict: Vec<HashMap<&str, String>> = simulated
                .menage_input
                .0
                .liste_menage_valide
                .iter()
                .map(|menage| {
                    menage
                        .caracteristiques
                        .iter()
                        .map(|(k, v)| (k.as_str(), format!("{}", v)))
                        .collect()
                })
                .collect();

            let joined_menages_diff_results: Vec<HashMap<&str, String>> = text_caract_dict
                .into_iter()
                .zip(text_results_dict)
                .map(|(mut caract_dict, results_dict)| {
                    caract_dict.extend(results_dict);
                    caract_dict
                })
                .collect();

            let mut wtr_diff = WriterBuilder::new()
                .delimiter(b';')
                .from_path(output_path_diff)
                .map_err(OutputAdapterError::from)?;

            let mut headers = simulated
                .policy_variante
                .0
                .valid_policy
                .composantes_ordonnees
                .iter()
                .map(|composante| composante.name.as_str())
                .chain(
                    simulated
                        .menage_input
                        .0
                        .set_caracteristiques_valide
                        .iter()
                        .map(|caracteristique_name| caracteristique_name.as_str()),
                )
                .collect::<Vec<&str>>();

            headers.sort_unstable();

            headers.insert(0, "Index");

            wtr_diff.write_record(&headers);

            headers.remove(0);

            for (index, results_menage_diff) in joined_menages_diff_results.iter().enumerate() {
                let mut vec_results_menage_diff = vec![];
                for name in headers.iter() {
                    let out_diff_result = results_menage_diff
                        .get(name)
                        .ok_or_else(|| {
                            SimulationError::ResultsError(format!(
                            "Problème de cohérence des composantes lors de l'export. Erreur à la composante : {}",
                            name
                        ))
                        })?;

                    vec_results_menage_diff.push(out_diff_result);
                }

                let index_mod = (index + 1).to_string();
                vec_results_menage_diff.insert(0, &index_mod);

                wtr_diff.write_record(&vec_results_menage_diff);
            }

            wtr_diff.flush().map_err(OutputAdapterError::from)?;
            return Ok(());
        }

        Err(KalkotoError::SimError(SimulationError::ResultsError(
            "Pas possible d'exporter : les résultats n'ont pas encore été calculés".to_string(),
        )))
    }
}

impl OutputWriter for CSVOutputAdapter {
    fn export_baseline_results<E>(
        &self,
        simulated: &SimulatorBuilder<ValidMenageInput, ValidBaselineInput, E>,
    ) -> KalkotoResult<()> {
        let output_path = match &self.output_prefix {
            Some(output_prefix) => format!("{}-baseline-results.csv", output_prefix),
            _ => String::from("baseline-results.csv"),
        };

        if let Some(baseline_results) = &simulated.results_baseline {
            let text_results_dict: Vec<HashMap<&str, String>> = baseline_results
                .iter()
                .map(|menage_results| {
                    menage_results
                        .iter()
                        .map(|(k, v)| (k.as_str(), format!("{}", v)))
                        .collect()
                })
                .collect();

            let mut text_caract_dict: Vec<HashMap<&str, String>> = simulated
                .menage_input
                .0
                .liste_menage_valide
                .iter()
                .map(|menage| {
                    menage
                        .caracteristiques
                        .iter()
                        .map(|(k, v)| (k.as_str(), format!("{}", v)))
                        .collect()
                })
                .collect();

            let joined_menages_baseline_results: Vec<HashMap<&str, String>> = text_caract_dict
                .into_iter()
                .zip(text_results_dict)
                .map(|(mut caract_dict, results_dict)| {
                    caract_dict.extend(results_dict);
                    caract_dict
                })
                .collect();

            let mut wtr = WriterBuilder::new()
                .delimiter(b';')
                .from_path(output_path)
                .map_err(OutputAdapterError::from)?;

            let mut headers = simulated
                .policy_baseline
                .0
                .valid_policy
                .composantes_ordonnees
                .iter()
                .map(|composante| composante.name.as_str())
                .chain(
                    simulated
                        .menage_input
                        .0
                        .set_caracteristiques_valide
                        .iter()
                        .map(|caracteristique_name| caracteristique_name.as_str()),
                )
                .collect::<Vec<&str>>();

            headers.sort_unstable();

            headers.insert(0, "Index");

            wtr.write_record(&headers);

            headers.remove(0);

            for (index, results_menage) in joined_menages_baseline_results.iter().enumerate() {
                let mut vec_results_menage = vec![];
                for name in headers.iter() {
                    vec_results_menage.push(
                        results_menage
                            .get(name)
                            .ok_or_else(|| {
                                SimulationError::ResultsError(
                                    format!("Problème de cohérence des composantes lors de l'export. Erreur à la composante : {}",name)
                                )
                            })?);
                }

                let index_mod = (index + 1).to_string();
                vec_results_menage.insert(0, &index_mod);
                wtr.write_record(&vec_results_menage);
            }

            wtr.flush().map_err(OutputAdapterError::from)?;
            return Ok(());
        }

        Err(KalkotoError::SimError(SimulationError::ResultsError(
            "Pas possible d'exporter : les résultats n'ont pas encore été calculés".to_string(),
        )))
    }

    fn export_variante_and_diff_results(
        self,
        simulated: SimulatorBuilder<ValidMenageInput, ValidBaselineInput, ValidVarianteInput>,
    ) -> KalkotoResult<()> {
        let output_adapter = Arc::new(self);
        let simulated = Arc::new(simulated);

        let mut thread_handles = vec![];

        (0..2).for_each(|_| {
            let output_adapter = output_adapter.clone();
            let simulated = simulated.clone();
            thread_handles.push(thread::spawn(move || {
                output_adapter.export_variante_results(simulated)
            }));
        });

        for handle in thread_handles {
            handle.join().unwrap()?
        }

        Ok(())
    }
}
