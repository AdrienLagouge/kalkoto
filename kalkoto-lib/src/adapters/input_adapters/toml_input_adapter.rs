use crate::adapters::{PolicyAdapter, PolicyAdapterError};
use crate::entities::policy::{Composante, Parameters, Policy};
use crate::entities::policy_input::PolicyInput;
use crate::KalkotoResult;
use std::collections::{HashMap, HashSet};
use std::{
    error::Error,
    fs::{write, File},
    io::{self, Read},
    path::Path,
};
use toml::Table;

pub struct TomlInputAdapter<P: AsRef<Path>> {
    file_path: P,
}

impl<P: AsRef<Path>> TomlInputAdapter<P> {
    pub fn new(path: P) -> Self {
        Self { file_path: path }
    }

    pub fn populate_from_path(&self) -> KalkotoResult<(String, String, Vec<Composante>)> {
        let mut file_content = match std::fs::read_to_string(&self.file_path) {
            Ok(file_content) => file_content,
            Err(e) => return Err(From::from(PolicyAdapterError::IO(e))),
        };

        let policy_table: Table = match file_content.parse::<Table>() {
            Ok(policy_table) => policy_table,
            Err(e) => return Err(From::from(PolicyAdapterError::DeserializeError(e))),
        };

        let policy_name: String = policy_table
            .get("name")
            .ok_or(PolicyAdapterError::from(
                "Le champ name est manquant !".to_string(),
            ))?
            .clone()
            .try_into()
            .map_err(PolicyAdapterError::from)?;

        let policy_intitule: String = policy_table
            .get("intitule_long")
            .ok_or(PolicyAdapterError::from(
                "Le champ intitule_long est manquant !".to_string(),
            ))?
            .clone()
            .try_into()
            .map_err(PolicyAdapterError::from)?;

        let policy_composantes: Vec<Composante> = policy_table
            .get("composante")
            .ok_or(PolicyAdapterError::from(
                "Le champ composante est manquant !".to_string(),
            ))?
            .clone()
            .try_into()
            .map_err(PolicyAdapterError::from)?;

        Ok((policy_name, policy_intitule, policy_composantes))
    }
}

impl<P: AsRef<Path>> PolicyAdapter for TomlInputAdapter<P> {
    fn create_valid_policy_input(&self) -> KalkotoResult<PolicyInput> {
        let (name, intitule_long, mut composantes) = self.populate_from_path()?;

        composantes.sort_by_key(|c| c.logical_order);

        let mut policy_parameters_intitules = HashMap::new();
        let mut policy_parameters_values = HashMap::new();
        let mut policy_caracteristiques = HashSet::new();

        for composante in composantes.iter() {
            let temp_dict_names: HashMap<String, String> = composante
                .parameters
                .names
                .iter()
                .zip(composante.parameters.intitules_long.iter())
                .map(|(name, intitule)| (name.clone(), intitule.clone()))
                .collect();
            policy_parameters_intitules.extend(temp_dict_names);

            let temp_dict_values: HashMap<String, f64> = composante
                .parameters
                .names
                .iter()
                .zip(composante.parameters.values.iter())
                .map(|(name, intitule)| (name.clone(), *intitule))
                .collect();
            policy_parameters_values.extend(temp_dict_values);

            let temp_set: HashSet<String> = composante
                .caracteristiques_dependencies
                .iter()
                .cloned()
                .collect();
            policy_caracteristiques.extend(temp_set);
        }

        let policy = Policy {
            name,
            intitule_long,
            composantes_ordonnees: composantes,
            parameters_intitules: policy_parameters_intitules.clone(),
            parameters_values: policy_parameters_values.clone(),
            caracteristiques_menages: policy_caracteristiques.clone(),
            python_functions: None,
        };

        let policy = policy.populate_python_functions()?;

        Ok(PolicyInput {
            valid_policy: policy,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempdir::TempDir;

    #[test]
    fn unvalid_toml_file_missing_name_populate() -> KalkotoResult<()> {
        static UNVALID_TOML_BYTES: &[u8] = r#"
intitule_long = "Aide personnalisée à domicile"

[[composante]]
name = "plan_notif"
intitule_long = "Plan notifié"
parameters.names = ["tau_1","tau_2"]
parameters.intitules_long = ["Taux GIR 1","Taux GIR 2"]
parameters.values = [0.15,0.3]
caracteristiques_dependencies = ["Age","GIR"]
logical_order = 1
function = """
def plan_notif(Variables, ParamsDict, MenageCarac):
    if MenageCarac["Age"] > 60 iand MenageCarac["GIR"] == 1:
        output = Variables["x"] + ParamsDict["tau_1"] * Variables["y"] + Variables["z"]
    else if MenageCarac["Age"] > 60 iand MenageCarac["GIR"] == 2:
        output = Variables["x"] + ParamsDict["tau_1"] * Variables["y"] + Variables["z"]
    else:
        output = 0.0
    return output
"""    
        "#
        .as_bytes();

        let tmp_dir = TempDir::new("test-input").map_err(PolicyAdapterError::IO)?;
        let file_path = tmp_dir.path().join("valid_toml.toml");
        let mut tmp_file = File::create(&file_path).map_err(PolicyAdapterError::IO)?;
        fs::write(&file_path, UNVALID_TOML_BYTES).map_err(PolicyAdapterError::IO)?;

        let toml_adapter = TomlInputAdapter::new(file_path);

        let wanted = true;

        let result = toml_adapter.create_valid_policy_input().is_err();
        assert_eq!(wanted, result);

        drop(tmp_file);
        tmp_dir.close().map_err(PolicyAdapterError::IO)?;

        Ok(())
    }

    #[test]
    fn unvalid_toml_file_missing_composante_populate() -> KalkotoResult<()> {
        static UNVALID_TOML_BYTES: &[u8] = r#"
intitule_long = "Aide personnalisée à domicile"

[[komposante]]
name = "plan_notif"
intitule_long = "Plan notifié"
parameters.names = ["tau_1","tau_2"]
parameters.intitules_long = ["Taux GIR 1","Taux GIR 2"]
parameters.values = [0.15,0.3]
caracteristiques_dependencies = ["Age","GIR"]
logical_order = 1
function = """
def plan_notif(Variables, ParamsDict, MenageCarac):
    if MenageCarac["Age"] > 60 iand MenageCarac["GIR"] == 1:
        output = Variables["x"] + ParamsDict["tau_1"] * Variables["y"] + Variables["z"]
    else if MenageCarac["Age"] > 60 iand MenageCarac["GIR"] == 2:
        output = Variables["x"] + ParamsDict["tau_1"] * Variables["y"] + Variables["z"]
    else:
        output = 0.0
    return output
"""    
        "#
        .as_bytes();

        let tmp_dir = TempDir::new("test-input").map_err(PolicyAdapterError::IO)?;
        let file_path = tmp_dir.path().join("valid_toml.toml");
        let mut tmp_file = File::create(&file_path).map_err(PolicyAdapterError::IO)?;
        fs::write(&file_path, UNVALID_TOML_BYTES).map_err(PolicyAdapterError::IO)?;

        let toml_adapter = TomlInputAdapter::new(file_path);

        let wanted = true;

        let result = toml_adapter.create_valid_policy_input().is_err();
        assert_eq!(wanted, result);

        drop(tmp_file);
        tmp_dir.close().map_err(PolicyAdapterError::IO)?;

        Ok(())
    }

    #[test]
    fn unvalid_toml_file_typo_composante_populate() -> KalkotoResult<()> {
        static UNVALID_TOML_BYTES: &[u8] = r#"
intitule_long = "Aide personnalisée à domicile"

[[composante]]
name = "plan_notif"
intitule_long = "Plan notifié"
parameters.names = ["tau_1","tau_2"]
parameters.intitules_long = ["Taux GIR 1","Taux GIR 2"]
parameters.values = [0.15,0.3]
caracteristiques_dependencies = ["Age","GIR"]
logical_order = 1
function = """
def plan_notif(Variables, ParamsDict, MenageCarac):
    if MenageCarac["Age"] > 60 iand MenageCarac["GIR"] == 1:
        output = Variables["x"] + ParamsDict["tau_1"] * Variables["y"] + Variables["z"]
    else if MenageCarac["Age"] > 60 iand MenageCarac["GIR"] == 2:
        output = Variables["x"] + ParamsDict["tau_1"] * Variables["y"] + Variables["z"]
    else:
        output = 0.0
    return output
"""    

[[komposante]]
name = "plan_cons"
intitule_long = "Plan effectivement consommé"
parameters.names = ["taux_ss_conso"]
parameters.intitules_long = ["Taux de sous-consommation du plan notifié"]
parameters.values = [0.8]
caracteristiques_dependencies = []
logical_order = 2
function = """
def plan_cons(Variables, ParamsDict, MenageCarac):
    output = Variables["plan_notif"]*ParamsDict["taux_ss_conso"]
    return output
"""
        "#
        .as_bytes();

        let tmp_dir = TempDir::new("test-input").map_err(PolicyAdapterError::IO)?;
        let file_path = tmp_dir.path().join("valid_toml.toml");
        let mut tmp_file = File::create(&file_path).map_err(PolicyAdapterError::IO)?;
        fs::write(&file_path, UNVALID_TOML_BYTES).map_err(PolicyAdapterError::IO)?;

        let toml_adapter = TomlInputAdapter::new(file_path);

        let wanted = true;

        let result = toml_adapter.create_valid_policy_input().is_err();
        assert_eq!(wanted, result);

        drop(tmp_file);
        tmp_dir.close().map_err(PolicyAdapterError::IO)?;

        Ok(())
    }
    #[test]
    fn err_toml_not_valid_file_path() -> KalkotoResult<()> {
        let result = TomlInputAdapter::new("nonexistent_path.toml")
            .create_valid_policy_input()
            .is_err();

        let wanted = true;

        assert_eq!(wanted, result);

        Ok(())
    }
}
