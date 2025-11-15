use crate::adapters::input_adapters::{MenageListCreator, PolicyCreator};
use crate::adapters::output_adapters::OutputWriter;
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
    MenagesPolicyMismatchError(String),
    
    #[error("Problème lors de l'exécution d'une fonction Python :\n\t\t -> {err_msg}\n\t\t -> {source}")]
    PythonError {
        source: pyo3::prelude::PyErr,
        err_msg: String
        },

    #[error("Résultats non valides : {0}")]
    ResultsError(String),
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
    pub results_diff: Option<Vec<HashMap<String, Option<f64>>>>,
}


impl SimulatorBuilder<EmptyMenageInput, EmptyBaselineInput, EmptyVarianteInput> {
    pub fn new() -> Self {
        SimulatorBuilder::default()
    }

    pub fn add_menage_input<M: MenageListCreator>(
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
    pub fn add_valid_baseline_policy<P: PolicyCreator>(
        self,
        baseline_policy_adapter: P,
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
                Err(KalkotoError::SimError(SimulationError::MenagesPolicyMismatchError(error_msg)))
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

    pub fn export_baseline<O: OutputWriter>(
        &self,
        output_adapter: &O,
    ) -> KalkotoResult<()> {
        output_adapter.export_baseline_results(self)
    }
}

impl SimulatorBuilder<ValidMenageInput, ValidBaselineInput, EmptyVarianteInput> {
    pub fn add_valid_variante_policy<P: PolicyCreator>(
        mut self,
        variante_policy_adapter: P,
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
            _ => Err(KalkotoError::SimError(SimulationError::MenagesPolicyMismatchError("Les caractéristiques dont dépend la politique variante sont plus larges que celles présentes dans le fichier ménages".to_string()))),
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
            .ok_or(SimulationError::ResultsError(
                "Baseline pas encore calculée !".to_string(),
            ))?
            .iter()
            .zip(results.iter())
        {
            let mut diff_map = HashMap::<String, Option<f64>>::new();

            for (variante_composante_name, variante_composante_value) in variante_result {
                let baseline_composante_value = baseline_result.get(variante_composante_name);
                let diff = baseline_composante_value.map( 
                    |baseline_value| variante_composante_value - baseline_value)
                    ;
                diff_map.insert(variante_composante_name.to_owned(), diff);
            }
            diff_results.push(diff_map);
        }

        self.results_variante = Some(results);
        self.results_diff = Some(diff_results);

        Ok(())
    }

    pub fn export_variante<O: OutputWriter>(
        &self,
        output_adapter: &O,
    ) -> KalkotoResult<()> {
        output_adapter.export_variante_results(self)
    }
}
