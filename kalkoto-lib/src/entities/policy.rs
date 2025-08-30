use crate::{entities::menage::Menage, prelude::*};
use pyo3::{prelude::*, types::IntoPyDict, types::PyDict, types::PyList};
use pyo3_ffi::c_str;
use rayon::prelude::*;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::ffi::CString;

#[derive(Deserialize, Debug, Clone)]
pub struct Parameters {
    pub names: Vec<String>,
    pub intitules_long: Vec<String>,
    pub values: Vec<f64>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Function(String);

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
    pub fn simulate_all_menages<'a>(
        &self,
        menages: &Bound<'a, PyList>,
        vec_variables_dict: &mut Bound<'a, PyList>,
        parameters_dict: &Bound<'a, PyDict>,
    ) -> PyResult<()> {
        Python::with_gil(|py| -> PyResult<()> {
            let composantemodule = PyModule::from_code(
                py,
                CString::new(self.function.0.to_owned())?.as_c_str(),
                c_str!("composantemodule.py"),
                c_str!("composantemodule"),
            )?;

            let rustfunc = composantemodule.getattr(&self.name)?;

            for index in 0..menages.len() {
                let args = (
                    &vec_variables_dict.get_item(index)?,
                    parameters_dict,
                    &menages.get_item(index)?,
                );

                let result = rustfunc.call(args, None);

                match result {
                    Ok(result) => {
                        vec_variables_dict
                            .get_item(index)?
                            .set_item(self.name.to_owned(), result)?;
                    }
                    Err(e) => println!(
                        "Erreur lors du calcul de la composante {} ; {}",
                        self.name,
                        e.to_string()
                    ),
                };
            }
            Ok(())
        })
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
}

impl Policy {
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

        pyo3::prepare_freethreaded_python();
        let output = Python::with_gil(|py| -> PyResult<Vec<HashMap<String, f64>>> {
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

            self.composantes_ordonnees
                .iter()
                .map(|composante: &Composante| {
                    composante.simulate_all_menages(
                        &menages_dicts,
                        &mut variables_dicts,
                        &params_dict_py,
                    )
                })
                .collect::<Vec<_>>();

            Ok(variables_dicts.extract()?)
        });
        Ok(output?)
    }
}
