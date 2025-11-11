use crate::entities::menage::Menage;
use crate::KalkotoResult;
use crate::adapters::input_adapters::MenageListAdapterError;
use crossterm::style::Stylize;
use itertools::Itertools;
use std::collections::HashSet;
use std::fmt::Display;

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

#[derive(Debug, Clone, PartialEq, Default)]
pub struct MenageInput {
    pub set_caracteristiques_valide: HashSet<String>,
    pub liste_menage_valide: Vec<Menage>,
}

impl MenageInput {
    pub fn get_valid_input_menages(self) -> (HashSet<String>, Vec<Menage>) {
        (self.set_caracteristiques_valide, self.liste_menage_valide)
    }
}

impl Display for MenageInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "{}",
            "Input Ménages correctement initialisé !\n".green().bold()
        )?;
        writeln!(
            f,
            "Liste des caractéristiques trouvées dans l'input Ménages :\n{:?}\n",
            self.set_caracteristiques_valide
        )?;
        writeln!(
            f,
            "Exemple du premier ménage trouvé dans l'input Ménages :\n{:?}",
            self.liste_menage_valide[0]
        )
    }
}
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
        invalid_liste_menage: &[Menage],
    ) -> MenageInputBuilder<Unvalid> {
        MenageInputBuilder {
            set_caracteristiques: None,
            liste_menage: Unvalid(invalid_liste_menage.to_owned()),
        }
    }
}

impl MenageInputBuilder<Unvalid> {
    pub fn has_valid_liste_menage(&self) -> KalkotoResult<bool> { 
    let unvalidated_liste_menage = &self.liste_menage.0;

        if unvalidated_liste_menage.is_empty() {
            return Err(From::from(MenageListAdapterError::Validation {
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
            return Err(crate::errors::KalkotoError::ListMenageError(MenageListAdapterError::Validation {
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
        Err(From::from(MenageListAdapterError::Validation { fault_index: -1 
            , cause: "La liste des caractéristiques des ménages ne peut pas être établie à partir de la liste des ménages".to_string(), conseil: "Vérifier la liste des étapes pour construire un MenageInput".to_string()}))
    }}
}
