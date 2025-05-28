use crate::{entities::menage::Menage, prelude::*};
use pyo3::{prelude::*, types::IntoPyDict, types::PyDict, types::PyList};
use pyo3_ffi::c_str;
use rayon::prelude::*;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::ffi::CString;
use std::rc::Rc;

use super::menage::Caracteristique;

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
    pub fn simulate_all_menages(
        &self,
        menages: &[Menage],
        vec_variables_dict: &mut [HashMap<String, f64>],
        parameters_dict: &HashMap<String, f64>,
    ) -> KalkotoResult<()> {
        let output = Python::with_gil(|py| -> PyResult<()> {
            let composantemodule = PyModule::from_code(
                py,
                CString::new(self.function.0.to_owned())?.as_c_str(),
                c_str!("composantemodule.py"),
                c_str!("composantemodule"),
            )?;

            let rustfunc = composantemodule.getattr(&self.name)?;

            let params_dict_py = parameters_dict.into_py_dict(py)?;

            let vec_menage_caract_dict: PyResult<Vec<Bound<'_, PyDict>>> = menages
                .iter()
                .map(|m| &m.caracteristiques)
                .map(|menage_caract| menage_caract.clone().into_py_dict(py))
                .collect();

            let vec_menage_caract_dict = &vec_menage_caract_dict?;

            for index in 0..menages.len() {
                let variables_dict = &mut vec_variables_dict[index];
                let variables_dict_py = (*variables_dict).clone().into_py_dict(py)?;
                if let Some(menage_caract_dict_py) = vec_menage_caract_dict.get(index) {
                    let args = (&variables_dict_py, &params_dict_py, menage_caract_dict_py);

                    let result = rustfunc.call(args, None);

                    match result {
                        Ok(result) => {
                            let output_py = result.extract()?;
                            let variables_dict_menage = &mut vec_variables_dict[index];
                            (*variables_dict_menage).insert(self.name.to_owned(), output_py);
                        }
                        Err(e) => return Err(e),
                    }
                };
            }
            Ok(())
        });
        Ok(output?)
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
        let mut vec_variables_dict: Vec<HashMap<String, f64>> = vec![HashMap::new(); menages.len()];

        self.composantes_ordonnees.iter().for_each(|composante| {
            composante.simulate_all_menages(
                menages,
                &mut vec_variables_dict,
                &self.parameters_values,
            );
        });

        Ok(vec_variables_dict)
    }
}
