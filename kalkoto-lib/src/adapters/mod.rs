use crate::entities::menage::Menage;
use itertools::Itertools;
use std::{error::Error, fmt::Debug};

mod csv_input_adapter;
mod toml_input_adapter;

#[derive(thiserror::Error)]
pub enum MenageListAdapterError {
    #[error("Erreur à la lecture du stream d'input")]
    IO(#[from] std::io::Error),

    #[error("Erreur à la validation de la liste des cas-types pour les ménages {} et {}. Cause : {}",.0,.0+1,.1)]
    ValidationError(i32, String),
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

#[derive(Debug, Clone)]
pub struct MenageInput {
    liste_menage_valide: Vec<Menage>,
}

// Trait commun à tous les adapteurs de création d'une liste de ménages dont toutes
// les caractéristiques ont été vérifiées
pub trait MenageListAdapter {
    fn create_valid_menage_input(
        &self,
        empty_menage_input: MenageInputBuilder<Empty>,
    ) -> Result<MenageInput, MenageListAdapterError>;
}

#[derive(Default, Clone)]
pub struct Empty;
#[derive(Default, Clone)]
pub struct Unvalid(Vec<Menage>);
#[derive(Clone, Default)]
pub struct Valid(Vec<Menage>);

#[derive(Debug, Clone, Default)]
pub struct MenageInputBuilder<U> {
    liste_menage: U,
}

impl MenageInputBuilder<Empty> {
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

        let mut first_faulty_menage: Vec<_> = unvalidated_liste_menage
            .iter()
            .tuple_windows::<(&Menage, &Menage)>()
            .filter(|pair| !(pair.0.compare_type_carac(pair.1).0))
            .map(|pair| pair.0.clone())
            .take(1)
            .collect();

        if let Some(menage) = first_faulty_menage.pop() {
            return Err(MenageListAdapterError::ValidationError(
                menage.index,
                "Les types des caractéristiques de ces deux ménages ne correspondent pas"
                    .to_owned(),
            ));
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
