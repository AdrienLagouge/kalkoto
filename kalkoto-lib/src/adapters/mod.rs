use crate::entities::menage::*;
use crate::entities::policy::*;
use crate::prelude::*;
use itertools::Itertools;
use std::boxed;
use std::fmt::Display;
use std::fs::write;
use std::{
    collections::HashSet,
    error::Error,
    fmt::{Debug},
};
use crossterm::style::Stylize;

pub mod csv_input_adapter;
pub mod toml_input_adapter;

#[derive(thiserror::Error)]
pub enum MenageListAdapterError {
    #[error("Erreur à la lecture du stream d'input ménages")]
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

#[derive(Debug, Clone, PartialEq,Default)]
pub struct MenageInput {
   pub set_caracteristiques_valide: HashSet<String>,
   pub liste_menage_valide: Vec<Menage>,
}

impl MenageInput {
    pub fn get_valid_input_menages(self) -> (HashSet<String>,Vec<Menage>) {
        (self.set_caracteristiques_valide,self.liste_menage_valide)
    }
}

impl Display for MenageInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f,"{}","Input Ménages correctement initialisé !\n".green().bold())?;
        writeln!(f, "Liste des caractéristiques trouvées dans l'input Ménages :\n{:?}\n", self.set_caracteristiques_valide)?;
        writeln!(f, "Exemple du premier ménage trouvé dans l'input Ménages :\n{:?}", self.liste_menage_valide[0])
    }
}

// Trait commun à tous les adapteurs de création d'une liste de ménages dont toutes
// les caractéristiques ont été vérifiées
pub trait MenageListAdapter {
    fn create_valid_menage_input(
        self,
        empty_menage_input: MenageInputBuilder<EmptyList>,
    ) -> KalkotoResult<MenageInput>;
}

//Marker trait pour définir les différents états possibles d'une liste ménages
pub trait MenageList {}

#[derive(Default, Debug, Clone)]
pub struct EmptyList;
impl MenageList for EmptyList {}
#[derive(Debug, Clone)]
pub struct Unvalid(Vec<Menage>);
impl MenageList for Unvalid {}
#[derive(Debug, Clone)]
pub struct Valid(Vec<Menage>);
impl MenageList for Valid {}

#[derive(Debug, Clone, Default)]
pub struct MenageInputBuilder<U: MenageList> {
    set_caracteristiques: Option<HashSet<String>>,
    liste_menage: U,
}

impl MenageInputBuilder<EmptyList> {
    pub fn new() -> Self {
        MenageInputBuilder::default()
    }
}

impl<U> MenageInputBuilder<U> where U: MenageList{
    pub fn from_unvalidated_liste_menage(
        self,
        invalid_liste_menage: Vec<Menage>,
    ) -> MenageInputBuilder<Unvalid> {
        MenageInputBuilder {
            set_caracteristiques: None,
            liste_menage: Unvalid(invalid_liste_menage),
        }
    }
}

impl MenageInputBuilder<Unvalid> {
    pub fn has_valid_liste_menage(&self) -> KalkotoResult<bool> { 
    let unvalidated_liste_menage = &self.liste_menage.0;

        if unvalidated_liste_menage.is_empty() {
            return Err(From::from(MenageListAdapterError::ValidationError {
                fault_index: -1,
                cause: "La liste de ménages à valider est vide.".to_string(),
                conseil: "Vérifier le fichier d'input".to_string(),
            }));
        }

        let mut first_faulty_menage: Vec<_> = unvalidated_liste_menage
            .iter()
            .tuple_windows::<(&Menage, &Menage)>()
            .map(|pair| pair.0.compare_type_carac(pair.1))
            .filter(|faulty_menages| !(faulty_menages.0))
            .take(1)
            .collect();

        if let Some(first_faulty_menage) = first_faulty_menage.pop() {
            let carac_cause = format!("Les types ou les noms des caractéristiques de ces deux ménages ne correspondent pas. Problème à la caractéristique {0}",first_faulty_menage.2.clone());
            return Err(crate::errors::KalkotoError::ListMenageError(MenageListAdapterError::ValidationError {
                fault_index: first_faulty_menage.1,
                cause:  carac_cause,
                conseil: "Vérifier le fichier d'input".to_owned(),
            }));
        };

     Ok(true)
    }

    pub fn validate_liste_menage(self) -> KalkotoResult<MenageInputBuilder<Valid>> {
        let valid_liste_menage = self.has_valid_liste_menage()?;

        let validated_set_caracteristiques: HashSet<String> = self.liste_menage.0.first().unwrap()
            .caracteristiques
            .keys()
            .cloned()
            .collect();

        Ok(MenageInputBuilder {
            set_caracteristiques: Some(validated_set_caracteristiques),
            liste_menage: Valid(self.liste_menage.0),
        })
    }
}

impl MenageInputBuilder<Valid> {
    pub fn build_valide_menage_input(self) -> KalkotoResult<MenageInput> {
        if let Some(set_caracteristiques) = self.set_caracteristiques {
            let liste_menage_valide = self.liste_menage.0;
            Ok(MenageInput {
                set_caracteristiques_valide: set_caracteristiques,
                liste_menage_valide,
            })
        } else {
        Err(From::from(MenageListAdapterError::ValidationError { fault_index: -1 
            , cause: "La liste des caractéristiques des ménages ne peut pas être établie à partir de la liste des ménages".to_string(), conseil: "Vérifier la liste des étapes pour construire un MenageInput".to_string()}))
    }}
}

#[derive(thiserror::Error,Debug)]
pub enum PolicyAdapterError {
    #[error("Erreur à l'ouverture du fichier")]
    IO(#[from] std::io::Error),

    #[error("Erreur à la lecture du fichier TOML")]
    DeserializeError(#[from] toml::de::Error),

    #[error("Champ(s) manquant(s) ou invalide(s): {0}")]
    Generic(String),
}

impl From<String> for PolicyAdapterError {
    fn from(value: String) -> Self {
        PolicyAdapterError::Generic(value)
    }
}

#[derive(Debug, Clone)]
pub struct PolicyInput {
    pub valid_policy: Policy
}

impl Display for PolicyInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f,"{}","Input Policy correctement initialisé !\n".green().bold())?;
        writeln!(f, "Politique publique à simuler  trouvée dans l'input Policy :\n{:?}\n", self.valid_policy.intitule_long)?;
        writeln!(f, "Liste ordonnée des composantes de cette politique publique :")?;
        let composantes_names  = self.valid_policy.composantes_ordonnees.iter().map(|s| format!("- {}",s.name)).collect::<Vec<String>>().join("\n");
        writeln!(f,"{}",composantes_names)
    }
}

// Trait commun à tous les adapteurs de création d'une politique publique correctement initialisée 
pub trait PolicyAdapter {
    fn create_valid_policy_input(
        &self
    ) -> KalkotoResult<PolicyInput>;
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::entities::menage::*;

    #[test]
    fn ok_valid_input() -> KalkotoResult<()>{
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
            .from_unvalidated_liste_menage(valide_menage_list)
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
            .from_unvalidated_liste_menage(invalid_menage_list)
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
