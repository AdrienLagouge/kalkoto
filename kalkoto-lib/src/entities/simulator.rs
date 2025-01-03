use crate::adapters::{MenageInput, PolicyInput};
use crate::entities::menage::Menage;
use crate::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fmt::write;

#[derive(Debug)]
pub struct Simulator {
    menage_input: MenageInput,
    policy_baseline: PolicyInput,
    policy_variante: Option<PolicyInput>,
    results_baseline: Vec<HashMap<String, f64>>,
    results_variante: Option<Vec<HashMap<String, f64>>>,
    results_diff: Option<Vec<HashMap<String, f64>>>,
    output_prefix: Option<String>,
}

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
    menage_input: M,
    policy_baseline: B,
    policy_variante: V,
    results_baseline: Option<Vec<HashMap<String, f64>>>,
    results_variante: Option<Vec<HashMap<String, f64>>>,
    results_diff: Option<Vec<HashMap<String, f64>>>,
    output_prefix: Option<String>,
}

#[derive(Default, Clone)]
pub struct EmptyMenageInput;
#[derive(Clone)]
pub struct ValidMenageInput(MenageInput);

#[derive(Default, Clone)]
pub struct EmptyBaselineInput;
#[derive(Clone)]
pub struct ValidBaselineInput(PolicyInput);

#[derive(Default, Clone)]
pub struct EmptyVarianteInput;
#[derive(Clone)]
pub struct ValidVarianteInput(PolicyInput);

impl SimulatorBuilder<EmptyMenageInput, EmptyBaselineInput, EmptyVarianteInput> {
    pub fn new() -> Self {
        SimulatorBuilder::default()
    }

    pub fn add_menage(
        self,
        menage_input: MenageInput,
    ) -> SimulatorBuilder<ValidMenageInput, EmptyBaselineInput, EmptyVarianteInput> {
        SimulatorBuilder {
            menage_input: ValidMenageInput(menage_input),
            policy_baseline: self.policy_baseline,
            policy_variante: self.policy_variante,
            results_baseline: self.results_baseline,
            results_variante: self.results_variante,
            results_diff: self.results_diff,
            output_prefix: self.output_prefix,
        }
    }
}

impl SimulatorBuilder<ValidMenageInput, EmptyBaselineInput, EmptyVarianteInput> {
    pub fn add_valid_baseline_policy(
        self,
        baseline_policy: PolicyInput,
    ) -> KalkotoResult<SimulatorBuilder<ValidMenageInput, ValidBaselineInput, EmptyVarianteInput>>
    {
        let intersect_caracteristiques = baseline_policy
            .valid_policy
            .caracteristiques_menages
            .intersection(&self.menage_input.0.set_caracteristiques_valide)
            .cloned()
            .collect::<HashSet<String>>();

        let is_valid =
            intersect_caracteristiques == baseline_policy.valid_policy.caracteristiques_menages;

        match is_valid {
            true => Ok(SimulatorBuilder {
                menage_input: self.menage_input,
                policy_baseline: ValidBaselineInput(baseline_policy),
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

impl SimulatorBuilder<ValidMenageInput, ValidBaselineInput, EmptyVarianteInput> {
    pub fn add_valid_variante_policy(
        self,
        variante_policy: PolicyInput,
    ) -> KalkotoResult<SimulatorBuilder<ValidMenageInput, ValidBaselineInput, ValidVarianteInput>>
    {
        let intersect_caracteristiques = variante_policy
            .valid_policy
            .caracteristiques_menages
            .intersection(&self.menage_input.0.set_caracteristiques_valide)
            .cloned()
            .collect::<HashSet<String>>();

        let is_valid =
            intersect_caracteristiques == variante_policy.valid_policy.caracteristiques_menages;

        match is_valid {
            true => Ok(SimulatorBuilder {
                menage_input: self.menage_input,
                policy_baseline: self.policy_baseline,
                policy_variante: ValidVarianteInput(variante_policy),
                results_baseline: self.results_baseline,
                results_variante: self.results_variante,
                results_diff: self.results_diff,
                output_prefix: self.output_prefix,
            }),
            _ => Err(SimulationError::from("Les caractéristiques dont dépend la politique variante sont plus larges que celles présentes dans le fichier ménages".to_string()).into()),
        }
    }
}
