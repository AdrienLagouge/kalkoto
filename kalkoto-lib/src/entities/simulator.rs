use crate::adapters::input_adapters::{MenageListAdapter, PolicyAdapter};
use crate::adapters::output_adapters::OutputAdapter;
use crate::entities::menage_input::*;
use crate::entities::policy_input::*;
use crate::{KalkotoError, KalkotoResult};
use rayon::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    fmt::{write, Write},
    hash::Hash,
};

#[derive(thiserror::Error, Debug)]
pub enum SimulationError {
    #[error("Erreur à la mise en cohérence ménages/policy : {0}")]
    PolicyError(String),

    #[error("Erreur à l'écriture du fichier csv d'output")]
    CsvError(#[from] csv::Error),

    #[error("Erreur à l'ouverture du fichier csv d'output")]
    IOError(#[from] std::io::Error),

    #[error("Erreur lors de l'exécution du code Python")]
    PythonError(String),
}

impl From<String> for SimulationError {
    fn from(value: String) -> Self {
        SimulationError::PolicyError(value)
    }
}

#[derive(Default)]
pub struct EmptyMenageInput;
#[derive(Clone)]
pub struct ValidMenageInput(pub MenageInput);

#[derive(Default)]
pub struct EmptyBaselineInput;
#[derive(Clone)]
pub struct ValidBaselineInput(pub PolicyInput);

#[derive(Default)]
pub struct EmptyVarianteInput;
#[derive(Clone)]
pub struct ValidVarianteInput(pub PolicyInput);

#[derive(Default)]
pub struct SimulatorBuilder<M, B, V> {
    pub menage_input: M,
    pub policy_baseline: B,
    pub policy_variante: V,
    pub results_baseline: Option<Vec<HashMap<String, f64>>>,
    pub results_variante: Option<Vec<HashMap<String, f64>>>,
    pub results_diff: Option<Vec<HashMap<String, f64>>>,
}


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

        let diff_caracteristiques: HashSet<_> = baseline_policy_input
            .valid_policy
            .caracteristiques_menages
            .difference(&self.menage_input.0.set_caracteristiques_valide)
            .clone()
            .collect();

        let is_valid = diff_caracteristiques.is_empty();

        match is_valid {
            true => Ok(SimulatorBuilder {
                menage_input: self.menage_input,
                policy_baseline: ValidBaselineInput(baseline_policy_input),
                policy_variante: self.policy_variante,
                results_baseline: self.results_baseline,
                results_variante: self.results_variante,
                results_diff: self.results_diff,
            }),
            _ => {
                let error_msg = format!("Les caractéristiques dont dépend la politique baseline sont plus larges que celles présentes dans le fichier ménages.\nMauvaises caractéristiques : {:?}",diff_caracteristiques);
                Err(KalkotoError::SimError(SimulationError::from(error_msg)))
            }
        }
    }
}

impl<E> SimulatorBuilder<ValidMenageInput, ValidBaselineInput, E> {
    pub fn simulate_baseline_policy(&mut self) -> KalkotoResult<()> {
        let results = self
            .policy_baseline
            .0
            .valid_policy
            .simulate_all_menages(&self.menage_input.0.liste_menage_valide)?;

        self.results_baseline = Some(results);

        Ok(())
    }

    pub fn export_baseline<O: OutputAdapter>(
        &self,
        output_adapter: &O,
    ) -> KalkotoResult<()> {
        output_adapter.export_baseline_results(self)
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
                results_diff: self.results_diff            
            }),
            _ => Err(SimulationError::from("Les caractéristiques dont dépend la politique variante sont plus larges que celles présentes dans le fichier ménages".to_string()).into()),
        }
    }
}

impl SimulatorBuilder<ValidMenageInput, ValidBaselineInput, ValidVarianteInput> {
    pub fn simulate_variante_policy(&mut self) -> KalkotoResult<()> {
        let results = self
            .policy_variante
            .0
            .valid_policy
            .simulate_all_menages(&self.menage_input.0.liste_menage_valide)?;

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

    pub fn export_variante<O: OutputAdapter>(
        &self,
        output_adapter: &O,
    ) -> KalkotoResult<()> {
        output_adapter.export_variante_results(self)
    }
}
