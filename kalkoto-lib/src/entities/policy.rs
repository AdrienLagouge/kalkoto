use crate::adapters::input_adapters::PolicyAdapterError;
use crate::entities::menage::{Caracteristique, Menage};
use crate::entities::simulator::SimulationError;
use crate::{KalkotoError, KalkotoResult};
use crossterm::cursor::RestorePosition;
use pyo3::{prelude::*, types::IntoPyDict, types::PyDict, types::PyList};
use pyo3_ffi::c_str;
use rayon::prelude::*;
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    ffi::CString,
    sync::Mutex,
};

#[derive(Deserialize, Debug, Clone)]
pub struct Parameters {
    pub names: Vec<String>,
    pub intitules_long: Vec<String>,
    pub values: Vec<f64>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Function(String);

impl From<Function> for String {
    fn from(value: Function) -> Self {
        value.0
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Composante {
    pub name: String,
    pub intitule_long: String,
    pub parameters: Parameters,
    pub logical_order: i32,
    pub caracteristiques_dependencies: Vec<String>,
    pub function: Function,
}

impl Composante {
    pub fn simulate_all_menages<'py>(
        &self,
        py: Python<'py>,
        py_menages_caract_dict: &Vec<Bound<'py, PyDict>>,
        py_menages_variables_dict: &mut Vec<Bound<'py, PyDict>>,
        parameters_dict: &Bound<'py, PyDict>,
        python_functions_module: &Bound<'py, PyModule>,
    ) -> KalkotoResult<()> {
        let rustfunc = python_functions_module.getattr(&self.name).map_err(|e| {
            SimulationError::PythonError {
                source: e,
                err_msg: format!(
                    "Erreur lors de l'interprétation de la fonction Python de la composante {}",
                    &self.name
                ),
            }
        })?;

        let python_simulation_result = py_menages_caract_dict
            .iter()
            .zip(py_menages_variables_dict.iter())
            .try_for_each(|(py_menage_caract_dict, py_menage_variables_dict)| {
                let args = (
                    py_menage_variables_dict,
                    &parameters_dict,
                    py_menage_caract_dict,
                );

                let result = rustfunc.call(args, None);

                match result {
                    Ok(result) => {
                        (*py_menage_variables_dict).set_item(self.name.to_owned(), result);
                        Ok(())
                    }
                    Err(e) => Err(SimulationError::PythonError {
                        source: e,
                        err_msg: format!("Erreur lors du calcul de la composante {}", self.name),
                    }),
                }
            });

        Ok(python_simulation_result?)
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Policy {
    pub name: String,
    pub intitule_long: String,
    pub composantes_ordonnees: Vec<Composante>,
    pub parameters_intitules: HashMap<String, String>, //Ensemble des paramètres dont dépend la pol. publique
    pub parameters_values: HashMap<String, f64>, //Ensemble des paramètres dont dépend la pol. publique
    pub caracteristiques_menages: HashSet<String>, //Ensemble des caracteristiques dont dépend la pol. publique
    pub python_functions: Option<String>,
}

impl Policy {
    pub fn populate_python_functions(self) -> KalkotoResult<Policy> {
        match &self.composantes_ordonnees.len() {
            0 => Err(KalkotoError::PolicyError(PolicyAdapterError::Generic(
                "Le fichier input policy n'est pas lu !".into(),
            ))),
            _ => {
                let py_functions_str: String = self
                    .composantes_ordonnees
                    .iter()
                    .map(|composante| composante.function.clone().into())
                    .collect::<Vec<String>>()
                    .join("\n");

                Ok(Policy {
                    python_functions: Some(py_functions_str),
                    ..self
                })
            }
        }
    }

    pub fn simulate_all_menages(
        &self,
        menages: &[Menage],
    ) -> KalkotoResult<Vec<HashMap<String, f64>>> {
        if let Some(ref python_functions) = &self.python_functions {
            let mut empty_vec_variables_dict: HashMap<String, f64> =
                HashMap::with_capacity(self.composantes_ordonnees.len());

            self.composantes_ordonnees.iter().map(|composante| {
                empty_vec_variables_dict.insert(composante.name.to_owned(), 0 as f64);
            });

            let mut vec_variables_dict: Vec<HashMap<String, f64>> =
                vec![empty_vec_variables_dict; menages.len()];

            Python::initialize();

            let output = Python::attach(|py| -> KalkotoResult<Vec<HashMap<String, f64>>> {
                let composantemodule = PyModule::from_code(
                    py,
                    CString::new(python_functions.to_owned())
                        .map_err(|e| SimulationError::PythonError {
                            source: e.into(),
                            err_msg: "Problème de lecture des fonctions Python".into(),
                        })?
                        .as_c_str(),
                    c_str!("composantemodule.py"),
                    c_str!("composantemodule"),
                )
                .map_err(|e| SimulationError::PythonError {
                    source: e,
                    err_msg: "Erreur à la création du module Python".into(),
                })?;

                let params_dict_py =
                    self.parameters_values
                        .clone()
                        .into_py_dict(py)
                        .map_err(|e| SimulationError::PythonError {
                            source: e,
                            err_msg: "Erreur pour dictionnaire de paramètres".into(),
                        })?;

                let py_menages_dicts: KalkotoResult<Vec<Bound<'_, PyDict>>> = menages
                    .iter()
                    .map(|menage| {
                        menage
                            .caracteristiques
                            .clone()
                            .into_py_dict(py)
                            .map_err(|e| SimulationError::PythonError {
                                source: e,
                                err_msg: "Erreur pour dictionnaire de caractéristiques des ménages"
                                    .into(),
                            })
                            .map_err(KalkotoError::from)
                    })
                    .collect();

                let py_menages_dicts = py_menages_dicts?;

                let mut py_variables_dicts: KalkotoResult<Vec<Bound<'_, PyDict>>> =
                    vec_variables_dict
                        .iter()
                        .map(|dict| {
                            dict.clone()
                                .into_py_dict(py)
                                .map_err(|e| SimulationError::PythonError {
                                    source: e,
                                    err_msg: "Erreur pour dictionnaire de variables des ménages"
                                        .into(),
                                })
                                .map_err(KalkotoError::from)
                        })
                        .collect();

                let mut py_variables_dicts = py_variables_dicts?;

                self.composantes_ordonnees
                    .iter()
                    .try_for_each(|composante: &Composante| {
                        composante.simulate_all_menages(
                            py,
                            &py_menages_dicts,
                            &mut py_variables_dicts,
                            &params_dict_py,
                            &composantemodule,
                        )
                    })?;

                let final_results_variables_dict: KalkotoResult<Vec<HashMap<String, f64>>> =
                    py_variables_dicts
                        .into_iter()
                        .map(|result_wrapper| {
                            result_wrapper
                                .extract::<HashMap<String, f64>>()
                                .map_err(|e| SimulationError::PythonError {
                                    source: e,
                                    err_msg: "Erreur à l'extraction des résultats depuis Python"
                                        .into(),
                                })
                                .map_err(KalkotoError::from)
                        })
                        .collect();

                final_results_variables_dict
            })?;

            Ok(output)
        } else {
            Err(KalkotoError::PolicyError(PolicyAdapterError::Generic(
                "Fichier policy pas encore lu ! Les fonctions Python ne sont pas initialisées"
                    .into(),
            )))
        }
    }
}
