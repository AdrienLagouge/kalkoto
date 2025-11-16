use crate::adapters::input_adapters::PolicyAdapterError;
use crate::entities::menage::Menage;
use crate::entities::simulator::SimulationError;
use crate::{KalkotoError, KalkotoResult};
use itertools::Itertools;
use pyo3::types::PyTuple;
use pyo3::PythonVersionInfo;
use pyo3::{prelude::*, types::IntoPyDict, types::PyDict, types::PyList};
use pyo3_ffi::{c_str, PyEval_ReleaseLock};
use rayon::prelude::*;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::ffi::CString;
use std::thread;

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
        chunksize: usize,
        menages_caract_dict_list: &Bound<'py, PyList>,
        vec_variables_dict_list: &mut Bound<'py, PyList>,
        parameters_dict: &Bound<'py, PyDict>,
        policy_py_module: &Bound<'py, PyModule>,
        pool: &Bound<'py, PyAny>,
    ) -> PyResult<()> {
        Python::attach(|py| -> PyResult<()> {
            let rustfunc = policy_py_module.getattr(&self.name)?;

            //Préparation des arguments
            let list_args = PyList::empty(py);
            let name = self.name.clone();

            for index in 0..menages_caract_dict_list.len() {
                let menage_caract_dict = menages_caract_dict_list.get_item(index)?;
                let vec_variables_dict = vec_variables_dict_list.get_item(index)?;

                let args = PyTuple::new(
                    py,
                    [&vec_variables_dict, parameters_dict, &menage_caract_dict],
                )?;

                list_args.append(args)?;
            }

            let results = pool.call_method(
                "starmap",
                (rustfunc, list_args),
                Some(&[("chunksize", chunksize)].into_py_dict(py)?),
            )?;

            for index in 0..vec_variables_dict_list.len() {
                vec_variables_dict_list
                    .get_item(index)?
                    .set_item(&name, results.get_item(index)?)?;
            }

            Ok(())
        })
        // .map_err(|e| {
        //     KalkotoError::from(SimulationError::PythonError {
        //         source: e,
        //         err_msg: "Erreur calcul".into(),
        //     })
        // })
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
        let mut empty_vec_variables_dict: HashMap<String, f64> =
            HashMap::with_capacity(self.composantes_ordonnees.len());

        self.composantes_ordonnees.iter().map(|composante| {
            empty_vec_variables_dict.insert(composante.name.to_owned(), 0 as f64);
        });

        let mut vec_variables_dict: Vec<HashMap<String, f64>> =
            vec![empty_vec_variables_dict; menages.len()];

        let policy_py_module = self
            .composantes_ordonnees
            .iter()
            .map(|composante| composante.function.0.clone())
            .collect::<Vec<String>>()
            .join("\n");

        Python::initialize();

        let output = Python::attach(|py| -> PyResult<Vec<HashMap<String, f64>>> {
            let params_dict_py = self.parameters_values.clone().into_py_dict(py)?;

            let menages_dicts = PyList::new(
                py,
                &menages
                    .iter()
                    .map(|menage| menage.caracteristiques.clone().into_py_dict(py))
                    .collect::<PyResult<Vec<_>>>()?,
            )?;

            let mut variables_dicts = PyList::new(
                py,
                vec_variables_dict
                    .iter()
                    .map(|dict| dict.clone().into_py_dict(py))
                    .collect::<PyResult<Vec<_>>>()?,
            )?;

            let policy_py_module = PyModule::from_code(
                py,
                CString::new(policy_py_module)?.as_c_str(),
                c_str!("composantemodule.py"),
                c_str!("composantemodule"),
            )?;

            let num_workers = thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4);

            let mp = py.import("multiprocessing")?;
            let pool = mp.getattr("Pool")?.call1((num_workers,))?; // Création du pool de 3 workers

            let nombre_menages = menages_dicts.len();

            // Formule recommandée: nombre_menages / (num_workers * 4)
            // Le facteur 4 permet d'avoir environ 4 chunks par worker
            // Cela équilibre la charge tout en minimisant l'overhead
            let chunksize = (nombre_menages / (num_workers * 2)).max(1);

            let results = self
                .composantes_ordonnees
                .iter()
                .map(|composante: &Composante| {
                    composante.simulate_all_menages(
                        chunksize,
                        &menages_dicts,
                        &mut variables_dicts,
                        &params_dict_py,
                        &policy_py_module,
                        &pool,
                    )
                })
                .collect::<PyResult<Vec<_>>>()?;

            variables_dicts.extract()
        });

        let output = output.map_err(|e| {
            KalkotoError::from(SimulationError::PythonError {
                source: e,
                err_msg: "Erreur extraction".into(),
            })
        })?;

        Ok(output)
    }
}
