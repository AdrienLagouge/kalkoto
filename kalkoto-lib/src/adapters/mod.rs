use crate::entities::menage::Menage;
use itertools::Itertools;
use std::{error::Error, fmt::Debug};

mod csv_input_adapter;
mod toml_input_adapter;

#[derive(thiserror::Error)]
pub enum MenageListAdapterError {
    #[error("Erreur à la lecture du stream d'input")]
    IO(#[from] std::io::Error),

    #[error("Erreur à la validation de la liste des cas-types pour les ménages {} et {}.\nCause : {}.\nConseil : {}",.fault_index,.fault_index+1,.cause,.conseil)]
    ValidationError {
        fault_index: i32,
        cause: String,
        conseil: String,
    },
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

#[derive(Debug, Clone, PartialEq)]
pub struct MenageInput {
    liste_menage_valide: Vec<Menage>,
}

// Trait commun à tous les adapteurs de création d'une liste de ménages dont toutes
// les caractéristiques ont été vérifiées
pub trait MenageListAdapter {
    fn create_valid_menage_input(
        &self,
        empty_menage_input: MenageInputBuilder<EmptyList>,
    ) -> Result<MenageInput, MenageListAdapterError>;
}

#[derive(Default, Debug, Clone)]
pub struct EmptyList;
#[derive(Debug, Clone)]
pub struct Unvalid(Vec<Menage>);
#[derive(Debug, Clone)]
pub struct Valid(Vec<Menage>);

#[derive(Debug, Clone, Default)]
pub struct MenageInputBuilder<U> {
    liste_menage: U,
}

impl MenageInputBuilder<EmptyList> {
    pub fn new() -> Self {
        MenageInputBuilder::default()
    }
}

impl<U> MenageInputBuilder<U> {
    pub fn from_unvalidated_liste_menage(
        self,
        invalid_liste_menage: Vec<Menage>,
    ) -> MenageInputBuilder<Unvalid> {
        MenageInputBuilder {
            liste_menage: Unvalid(invalid_liste_menage),
        }
    }
}

impl MenageInputBuilder<Unvalid> {
    pub fn validate_liste_menage(
        self,
    ) -> Result<MenageInputBuilder<Valid>, MenageListAdapterError> {
        let unvalidated_liste_menage = &self.liste_menage.0;

        if unvalidated_liste_menage.is_empty() {
            return Err(MenageListAdapterError::ValidationError {
                fault_index: -1,
                cause: "La liste de ménages à valider est vide.".to_string(),
                conseil: "Vérifier le fichier d'input".to_string(),
            });
        }

        let mut first_faulty_menage: Vec<_> = unvalidated_liste_menage
            .iter()
            .tuple_windows::<(&Menage, &Menage)>()
            .filter(|pair| !(pair.0.compare_type_carac(pair.1).0))
            .map(|pair| pair.0.clone())
            .take(1)
            .collect();

        if let Some(menage) = first_faulty_menage.pop() {
            return Err(MenageListAdapterError::ValidationError {
                fault_index: menage.index,
                cause: "Les types ou les noms des caractéristiques de ces deux ménages ne correspondent pas"
                    .to_owned(),
                conseil: "Vérifier le fichier d'input".to_owned(),
            });
        };

        let validated_liste_menage = unvalidated_liste_menage.clone();

        Ok(MenageInputBuilder {
            liste_menage: Valid(validated_liste_menage),
        })
    }
}

impl MenageInputBuilder<Valid> {
    pub fn build_valide_menage_input(self) -> MenageInput {
        let liste_menage_valide = self.liste_menage.0;
        MenageInput {
            liste_menage_valide: liste_menage_valide.clone(),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::entities::menage::*;

    #[test]
    fn ok_valid_input() {
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

        let wanted = MenageInput {
            liste_menage_valide: valide_menage_list.clone(),
        };

        let result = MenageInputBuilder::<EmptyList>::new()
            .from_unvalidated_liste_menage(valide_menage_list)
            .validate_liste_menage()
            .unwrap()
            .build_valide_menage_input();
        assert_eq!(wanted, result);
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
            .from_unvalidated_liste_menage(invalid_menage_list.clone())
            .validate_liste_menage()
            .is_err();
        assert_eq!(wanted, result);
    }

    #[test]
    fn err_empty_list() {
        let wanted = true;
        let result = MenageInputBuilder::<EmptyList>::new()
            .from_unvalidated_liste_menage(vec![])
            .validate_liste_menage()
            .is_err();
        assert_eq!(wanted, result);
    }
}
