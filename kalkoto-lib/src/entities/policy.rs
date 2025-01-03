use serde::Deserialize;
use std::collections::{HashMap, HashSet};

#[derive(Deserialize, Debug)]
pub struct Parameters {
    pub names: Vec<String>,
    pub intitules_long: Vec<String>,
    pub values: Vec<f64>,
}

#[derive(Deserialize, Debug)]
pub struct Function(String);

#[derive(Deserialize, Debug)]
pub struct Composante {
    pub name: String,
    pub intitule_long: String,
    pub parameters: Parameters,
    pub logical_order: i32,
    pub caracteristiques_dependencies: Vec<String>,
    pub function: Function,
}

#[derive(Deserialize, Debug)]
pub struct Policy {
    pub name: String,
    pub intitule_long: String,
    pub composantes_ordonnees: Vec<Composante>,
    pub parameters_intitules: HashMap<String, String>, //Ensemble des paramètres dont dépend la pol. publique
    pub parameters_values: HashMap<String, f64>, //Ensemble des paramètres dont dépend la pol. publique
    pub caracteristiques_menages: HashSet<String>, //Ensemble des caracteristiques dont dépend la pol. publique
}
