use crate::adapters::*;
use crate::entities::menage::Menage;
use crate::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fmt::write;

#[derive(thiserror::Error, Debug)]
pub enum SimulationError {
    #[error("Erreur à la mise en cohérence ménages/policy : {0}")]
    PolicyError(String),
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
    pub fn add_output_prefix(self, prefix: String) -> Self {
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
        menage_input_adapter: &M,
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
    pub fn simulate_baseline_policy(self) -> KalkotoResult<Self> {
        let results: KalkotoResult<Vec<HashMap<String, f64>>> = self
            .menage_input
            .0
            .liste_menage_valide
            .iter()
            .map(|menage| self.policy_baseline.0.valid_policy.simulate_menage(menage))
            .collect();

        match results {
            Ok(results) => Ok(Self {
                results_baseline: Some(results.clone()),
                ..self
            }),
            Err(e) => Err(e),
        }
    }
}

impl SimulatorBuilder<ValidMenageInput, ValidBaselineInput, EmptyVarianteInput> {
    pub fn add_valid_variante_policy<P: PolicyAdapter>(
        self,
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
    pub fn simulate_variante_policy(self) -> KalkotoResult<Self> {
        let results: KalkotoResult<Vec<HashMap<String, f64>>> = self
            .menage_input
            .0
            .liste_menage_valide
            .iter()
            .map(|menage| self.policy_variante.0.valid_policy.simulate_menage(menage))
            .collect();

        let results = match results {
            Ok(results) => results,
            Err(e) => return Err(e),
        };

        let mut diff_results = vec![];

        for (baseline_result, variante_result) in self
            .results_baseline
            .clone()
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
                let diff = baseline_value - var_value;
                diff_map.insert(name.clone(), diff);
            }
            diff_results.push(diff_map);
        }

        Ok(Self {
            results_variante: Some(results),
            results_diff: Some(diff_results),
            ..self
        })
    }
}
