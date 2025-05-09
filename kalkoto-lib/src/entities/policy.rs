use crate::entities::menage::Menage;
use crate::prelude::*;
use pyo3::{prelude::*, types::IntoPyDict};
use pyo3_ffi::c_str;
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
    pub fn simulate_menage(
        &self,
        menage: &Menage,
        variables_dict: &mut HashMap<String, f64>,
        parameters_dict: &HashMap<String, f64>,
    ) -> KalkotoResult<()> {
        pyo3::prepare_freethreaded_python();
        let mut output: f64;
        output = Python::with_gil(|py| -> PyResult<f64> {
            let composantemodule = PyModule::from_code(
                py,
                CString::new(self.function.0.to_owned())?.as_c_str(),
                c_str!("composantemodule.py"),
                c_str!("composantemodule"),
            )?;

            let variables_dict_py = variables_dict.to_owned().into_py_dict(py)?;
            let params_dict_py = parameters_dict.into_py_dict(py)?;
            let menage_carac_dict_py = menage.caracteristiques.clone().into_py_dict(py)?;

            let args = (variables_dict_py, params_dict_py, menage_carac_dict_py);

            let rustfunc = composantemodule.getattr(&self.name)?;
            let result = rustfunc.call(args, None)?;
            let output_py = result.extract()?;

            Ok(output_py)
        })?;

        variables_dict.insert(self.name.to_owned(), output);

        Ok(())
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
    pub fn simulate_menage(&self, menage: &Menage) -> KalkotoResult<HashMap<String, f64>> {
        let mut variables_dict = HashMap::<String, f64>::new();

        for composante in self.composantes_ordonnees.iter() {
            composante.simulate_menage(menage, &mut variables_dict, &self.parameters_values)?;
        }

        Ok(variables_dict.to_owned())
    }
}
