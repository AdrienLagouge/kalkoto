use crate::adapters::*;
use crate::entities::menage::Menage;
use crate::prelude::*;
use csv::WriterBuilder;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fmt::{write, Write};
use std::hash::Hash;
use std::vec;

#[derive(thiserror::Error, Debug)]
pub enum SimulationError {
    #[error("Erreur à la mise en cohérence ménages/policy : {0}")]
    PolicyError(String),

    #[error("Erreur à l'écriture du fichier csv d'output")]
    CsvError(#[from] csv::Error),

    #[error("Erreur à l'ouverture du fichier csv d'output")]
    IOError(#[from] std::io::Error),
}

impl From<String> for SimulationError {
    fn from(value: String) -> Self {
        SimulationError::PolicyError(value)
    }
}

#[derive(Default)]
pub struct SimulatorBuilder<M, B, V> {
    pub menage_input: M,
    pub policy_baseline: B,
    pub policy_variante: V,
    pub results_baseline: Option<Vec<HashMap<String, f64>>>,
    pub results_variante: Option<Vec<HashMap<String, f64>>>,
    pub results_diff: Option<Vec<HashMap<String, f64>>>,
    pub output_prefix: Option<String>,
}

impl<M, B, V> SimulatorBuilder<M, B, V> {
    pub fn add_output_prefix(mut self, prefix: String) -> Self {
        Self {
            output_prefix: Some(prefix.clone()),
            ..self
        }
    }
}

#[derive(Default, Clone)]
pub struct EmptyMenageInput;
#[derive(Clone)]
pub struct ValidMenageInput(pub MenageInput);

#[derive(Default, Clone)]
pub struct EmptyBaselineInput;
#[derive(Clone)]
pub struct ValidBaselineInput(pub PolicyInput);

#[derive(Default, Clone)]
pub struct EmptyVarianteInput;
#[derive(Clone)]
pub struct ValidVarianteInput(pub PolicyInput);

impl SimulatorBuilder<EmptyMenageInput, EmptyBaselineInput, EmptyVarianteInput> {
    pub fn new() -> Self {
        SimulatorBuilder::default()
    }

    pub fn add_menage_input<M: MenageListAdapter>(
        self,
        menage_input_adapter: M,
    ) -> KalkotoResult<SimulatorBuilder<ValidMenageInput, EmptyBaselineInput, EmptyVarianteInput>>
    {
        let start_menage_list = MenageInputBuilder::<EmptyList>::new();
        let menage_input = menage_input_adapter.create_valid_menage_input(start_menage_list)?;

        Ok(SimulatorBuilder {
            menage_input: ValidMenageInput(menage_input),
            policy_baseline: self.policy_baseline,
            policy_variante: self.policy_variante,
            results_baseline: self.results_baseline,
            results_variante: self.results_variante,
            results_diff: self.results_diff,
            output_prefix: self.output_prefix,
        })
    }
}

impl SimulatorBuilder<ValidMenageInput, EmptyBaselineInput, EmptyVarianteInput> {
    pub fn add_valid_baseline_policy<P: PolicyAdapter>(
        self,
        baseline_policy_adapter: &P,
    ) -> KalkotoResult<SimulatorBuilder<ValidMenageInput, ValidBaselineInput, EmptyVarianteInput>>
    {
        let baseline_policy_input = baseline_policy_adapter.create_valid_policy_input()?;

        let intersect_caracteristiques = baseline_policy_input
            .valid_policy
            .caracteristiques_menages
            .intersection(&self.menage_input.0.set_caracteristiques_valide)
            .cloned()
            .collect::<HashSet<String>>();

        let is_valid = intersect_caracteristiques
            == baseline_policy_input.valid_policy.caracteristiques_menages;

        match is_valid {
            true => Ok(SimulatorBuilder {
                menage_input: self.menage_input,
                policy_baseline: ValidBaselineInput(baseline_policy_input),
                policy_variante: self.policy_variante,
                results_baseline: self.results_baseline,
                results_variante: self.results_variante,
                results_diff: self.results_diff,
                output_prefix: self.output_prefix,
            }),
            _ => Err(SimulationError::from("Les caractéristiques dont dépend la politique baseline sont plus larges que celles présentes dans le fichier ménages".to_string()).into()),
        }
    }
}

impl<E> SimulatorBuilder<ValidMenageInput, ValidBaselineInput, E> {
    pub fn simulate_baseline_policy(&mut self) -> KalkotoResult<()> {
        let results = self
            .menage_input
            .0
            .liste_menage_valide
            .par_iter()
            .map(|menage| self.policy_baseline.0.valid_policy.simulate_menage(menage))
            .collect::<KalkotoResult<Vec<HashMap<String, f64>>>>()?;

        self.results_baseline = Some(results);
        Ok(())
    }

    pub fn export_baseline_results_csv(&self) -> KalkotoResult<()> {
        let output_path = match &self.output_prefix {
            Some(output_prefix) => format!("{}-baseline-results.csv", output_prefix),
            _ => String::from("baseline-results.csv"),
        };

        if let Some(baseline_results) = &self.results_baseline {
            let mut wtr = WriterBuilder::new()
                .delimiter(b';')
                .from_path(output_path)
                .map_err(SimulationError::from)?;

            let mut headers = self
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
                            .ok_or(SimulationError::from(
                                "Problème de cohérence des composantes lors de l'export"
                                    .to_string(),
                            ))?
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
}

impl SimulatorBuilder<ValidMenageInput, ValidBaselineInput, EmptyVarianteInput> {
    pub fn add_valid_variante_policy<P: PolicyAdapter>(
        mut self,
        variante_policy_adapter: &P,
    ) -> KalkotoResult<SimulatorBuilder<ValidMenageInput, ValidBaselineInput, ValidVarianteInput>>
    {
        let variante_policy_input = variante_policy_adapter.create_valid_policy_input()?;

        let intersect_caracteristiques = variante_policy_input
            .valid_policy
            .caracteristiques_menages
            .intersection(&self.menage_input.0.set_caracteristiques_valide)
            .cloned()
            .collect::<HashSet<String>>();

        let is_valid = intersect_caracteristiques
            == variante_policy_input.valid_policy.caracteristiques_menages;

        match is_valid {
            true => Ok(SimulatorBuilder {
                menage_input: self.menage_input,
                policy_baseline: self.policy_baseline,
                policy_variante: ValidVarianteInput(variante_policy_input),
                results_baseline: self.results_baseline,
                results_variante: self.results_variante,
                results_diff: self.results_diff,
                output_prefix: self.output_prefix,
            }),
            _ => Err(SimulationError::from("Les caractéristiques dont dépend la politique variante sont plus larges que celles présentes dans le fichier ménages".to_string()).into()),
        }
    }
}

impl SimulatorBuilder<ValidMenageInput, ValidBaselineInput, ValidVarianteInput> {
    pub fn simulate_variante_policy(&mut self) -> KalkotoResult<()> {
        let results = self
            .menage_input
            .0
            .liste_menage_valide
            .par_iter()
            .map(|menage| self.policy_variante.0.valid_policy.simulate_menage(menage))
            .collect::<KalkotoResult<Vec<HashMap<String, f64>>>>()?;

        let mut diff_results = vec![];

        for (baseline_result, variante_result) in self
            .results_baseline
            .take()
            .ok_or(SimulationError::from(
                "Baseline pas encore calculée !".to_string(),
            ))?
            .iter()
            .zip(results.iter())
        {
            let mut diff_map = HashMap::<String, f64>::new();

            for (name, baseline_value) in baseline_result {
                let var_value = *variante_result.get(name).ok_or(SimulationError::from(
                    "Variante non encore calculée".to_string(),
                ))?;
                let diff = var_value - baseline_value;
                diff_map.insert(name.to_owned(), diff);
            }
            diff_results.push(diff_map);
        }

        self.results_variante = Some(results);
        self.results_diff = Some(diff_results);

        Ok(())
    }

    pub fn export_variante_results_csv(&self) -> KalkotoResult<()> {
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
            (&self.results_variante, &self.results_diff)
        {
            let mut wtr_var = WriterBuilder::new()
                .delimiter(b';')
                .from_path(output_path_var)
                .map_err(SimulationError::from)?;

            let mut wtr_diff = WriterBuilder::new()
                .delimiter(b';')
                .from_path(output_path_diff)
                .map_err(SimulationError::from)?;

            let mut headers = self
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
