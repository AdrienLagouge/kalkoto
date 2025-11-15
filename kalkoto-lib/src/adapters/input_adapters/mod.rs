use arrow::array::ArrowNumericType;
use pyo3::exceptions::PySyntaxWarning;

use crate::adapters::input_adapters::arrow_input_adapter::ArrowInputAdapter;
use crate::adapters::input_adapters::csv_input_adapter::CsvInputAdapter;
use crate::entities::menage::*;
use crate::entities::menage_input::*;
use crate::entities::policy::*;
use crate::entities::policy_input::PolicyInput;
use crate::KalkotoResult;
use std::fmt::Display;
use std::fs::write;
use std::{collections::HashSet, error::Error, fmt::Debug};

pub mod arrow_input_adapter;
pub mod csv_input_adapter;
pub mod toml_input_adapter;

pub enum MenageAdapter {
    CSV(CsvInputAdapter),
    Arrow(ArrowInputAdapter),
}

#[derive(thiserror::Error)]
pub enum MenageListAdapterError {
    #[error("Erreur à la lecture du stream d'input ménages")]
    IO(#[from] std::io::Error),

    #[error("Erreur de format de fichier d'input ménages\n\t\t->{0}")]
    FileFormat(String),

    #[error("Erreur à la validation de la liste des cas-types pour les ménages {} et {}.\nCause : {}.\nConseil : {}",.fault_index,.fault_index+1,.cause,.conseil)]
    Validation {
        fault_index: i32,
        cause: String,
        conseil: String,
    },

    #[error("Erreur à la lecture du dataframe Arrow")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Validation impossible : le fichier ménages n'a pas encore été initialisé !")]
    Uninitialized,
}

impl Debug for MenageListAdapterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}", self)?;
        if let Some(source) = self.source() {
            writeln!(f, "Cause : \n\t{}", source)?;
        }
        Ok(())
    }
}

// Trait commun à tous les adapteurs de création d'une liste de ménages dont toutes
// les caractéristiques ont été vérifiées
pub trait MenageListCreator {
    fn create_valid_menage_input(
        self,
        empty_menage_input: MenageInputBuilder<EmptyList>,
    ) -> KalkotoResult<MenageInput>;
}

impl MenageListCreator for MenageAdapter {
    fn create_valid_menage_input(
        self,
        empty_menage_input: MenageInputBuilder<EmptyList>,
    ) -> KalkotoResult<MenageInput> {
        match self {
            Self::CSV(csv_input_adapter) => {
                csv_input_adapter.create_valid_menage_input(empty_menage_input)
            }
            Self::Arrow(arrow_input_adapter) => {
                arrow_input_adapter.create_valid_menage_input(empty_menage_input)
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum PolicyAdapterError {
    #[error("Erreur à l'ouverture du fichier")]
    IO(#[from] std::io::Error),

    #[error("Erreur de format de fichier d'input policy\n\t\t->{0}")]
    FileFormat(String),

    #[error("Erreur à la lecture du fichier TOML")]
    UTF8(#[from] std::str::Utf8Error),

    #[error("Erreur au parsing du fichier TOML")]
    Deserialize(#[from] toml::ser::Error),

    #[error("Erreur à l'interprétation du fichier TOML")]
    Interpret(#[from] toml::de::Error),

    #[error("Champ(s) manquant(s) ou invalide(s): {0}")]
    Generic(String),

    #[error("Problème à la création de l'input modélisé")]
    Trait,
}

impl From<String> for PolicyAdapterError {
    fn from(value: String) -> Self {
        PolicyAdapterError::Generic(value)
    }
}

// Trait commun à tous les adapteurs de création d'une politique publique correctement initialisée
pub trait PolicyCreator {
    fn create_valid_policy_input(self) -> KalkotoResult<PolicyInput>;
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::entities::menage::*;

    #[test]
    fn ok_valid_input() -> KalkotoResult<()> {
        let mut first_menage = Menage::new(1);
        first_menage
            .caracteristiques
            .insert(String::from("Age"), Caracteristique::Entier(30));

        first_menage
            .caracteristiques
            .insert(String::from("Revenu"), Caracteristique::Numeric(100.0f64));

        let mut second_menage = Menage::new(2);
        second_menage
            .caracteristiques
            .insert(String::from("Age"), Caracteristique::Entier(35));

        second_menage
            .caracteristiques
            .insert(String::from("Revenu"), Caracteristique::Numeric(200.0f64));

        let mut third_menage = Menage::new(3);
        third_menage
            .caracteristiques
            .insert(String::from("Age"), Caracteristique::Entier(40));

        third_menage
            .caracteristiques
            .insert(String::from("Revenu"), Caracteristique::Numeric(300.0f64));

        let valide_menage_list = vec![first_menage, second_menage, third_menage];

        let mut valide_caracteristiques = HashSet::new();
        valide_caracteristiques.insert("Age".to_string());
        valide_caracteristiques.insert("Revenu".to_string());
        valide_caracteristiques.insert("Age".to_string());

        let wanted = MenageInput {
            set_caracteristiques_valide: valide_caracteristiques,
            liste_menage_valide: valide_menage_list.clone(),
        };

        let result = MenageInputBuilder::<EmptyList>::new()
            .from_unvalidated_liste_menage(&valide_menage_list)
            .validate_liste_menage()?
            .build_valide_menage_input()?;

        assert_eq!(wanted, result);

        Ok(())
    }

    #[test]
    fn err_invalid_input() {
        let mut first_menage = Menage::new(1);
        first_menage
            .caracteristiques
            .insert(String::from("Age"), Caracteristique::Entier(30));

        first_menage
            .caracteristiques
            .insert(String::from("Revenu"), Caracteristique::Numeric(100.0f64));

        let mut second_menage = Menage::new(2);
        second_menage
            .caracteristiques
            .insert(String::from("Age"), Caracteristique::Entier(35));

        second_menage
            .caracteristiques
            .insert(String::from("Revenu"), Caracteristique::Numeric(200.0f64));

        let mut third_menage = Menage::new(3);
        third_menage.caracteristiques.insert(
            String::from("Age"),
            Caracteristique::Textuel("40".to_string()),
        );

        third_menage
            .caracteristiques
            .insert(String::from("Revenu"), Caracteristique::Numeric(300.0f64));

        let invalid_menage_list = vec![first_menage, second_menage, third_menage];

        let wanted = true;
        let mut result = MenageInputBuilder::<EmptyList>::new()
            .from_unvalidated_liste_menage(&invalid_menage_list)
            .validate_liste_menage()
            .is_err();
        assert_eq!(wanted, result);
    }

    #[test]
    fn err_empty_list() {
        let wanted = true;
        let result = MenageInputBuilder::<EmptyList>::new()
            .from_unvalidated_liste_menage(&[])
            .validate_liste_menage()
            .is_err();
        assert_eq!(wanted, result);
    }
}
