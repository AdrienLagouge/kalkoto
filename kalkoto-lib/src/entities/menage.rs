use pyo3::prelude::*;
use std::collections::HashMap;
use std::fmt;
use std::mem;

#[derive(IntoPyObject, Clone, Debug, PartialEq)]
pub enum Caracteristique {
    Entier(i32),
    Numeric(f64),
    Textuel(String),
}

impl fmt::Display for Caracteristique {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Caracteristique::Entier(i) => write!(f, "{i}"),
            Caracteristique::Numeric(x) => write!(f, "{x}"),
            Caracteristique::Textuel(word) => write!(f, "{word}"),
        }
    }
}

impl From<String> for Caracteristique {
    fn from(string: String) -> Caracteristique {
        if let Ok(entier) = string.parse::<i32>() {
            Caracteristique::Entier(entier)
        } else if let Ok(numeric) = string.parse::<f64>() {
            Caracteristique::Numeric(numeric)
        } else {
            Caracteristique::Textuel(string)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Menage {
    pub index: i32,
    pub caracteristiques: HashMap<String, Caracteristique>,
}

impl Menage {
    pub fn new(index: i32) -> Self {
        Self {
            index,
            caracteristiques: HashMap::new(),
        }
    }

    pub fn compare_type_carac(&self, other_menage: &Self) -> (bool, i32, String) {
        let mut validator = true;
        let mut fault_index = -1;
        let mut fault_key = "".to_string();

        for (nom_carac, type_carac) in &self.caracteristiques {
            match other_menage.caracteristiques.get(nom_carac) {
                Some(other_type_carac) => {
                    validator = validator
                        && (mem::discriminant(type_carac) == mem::discriminant(other_type_carac))
                }
                None => {
                    validator = false;
                    fault_key = nom_carac.clone();
                }
            }
            if !validator {
                fault_index = self.index;
                return (validator, fault_index, fault_key);
            }
        }
        (validator, fault_index, fault_key)
    }
}

impl fmt::Display for Menage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Le ménage {} a les caractéristiques :", self.index)?;
        for (key, value) in &self.caracteristiques {
            writeln!(f, "{} -> {}", key, value)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ok_compar_carac() {
        let wanted = (true, -1, "".to_string());

        let mut first_menage = Menage::new(1);
        first_menage
            .caracteristiques
            .insert(String::from("Age"), Caracteristique::Entier(30));

        first_menage
            .caracteristiques
            .insert(String::from("Revenu"), Caracteristique::Numeric(500.65f64));

        let mut second_menage = Menage::new(2);
        second_menage
            .caracteristiques
            .insert(String::from("Age"), Caracteristique::Entier(30));

        second_menage
            .caracteristiques
            .insert(String::from("Revenu"), Caracteristique::Numeric(400.45f64));

        let result = first_menage.compare_type_carac(&second_menage);
        assert_eq!(wanted, result);
    }

    #[test]
    fn unmatched_types_compar_carac() {
        let wanted = (false, 1, "".to_string());

        let mut first_menage = Menage::new(1);
        first_menage
            .caracteristiques
            .insert(String::from("Age"), Caracteristique::Entier(30));

        first_menage
            .caracteristiques
            .insert(String::from("Revenu"), Caracteristique::Numeric(500.65f64));

        let mut second_menage = Menage::new(2);
        second_menage
            .caracteristiques
            .insert(String::from("Age"), Caracteristique::Entier(30));

        second_menage
            .caracteristiques
            .insert(String::from("Revenu"), Caracteristique::Entier(400));

        let result = first_menage.compare_type_carac(&second_menage);
        assert_eq!(wanted, result);
    }

    #[test]
    fn unmatched_nom_compar_carac() {
        let wanted = (false, 1, "TypeLogement".to_string());

        let mut first_menage = Menage::new(1);
        first_menage
            .caracteristiques
            .insert(String::from("Age"), Caracteristique::Entier(30));

        first_menage.caracteristiques.insert(
            String::from("TypeLogement"),
            Caracteristique::Textuel("Locataire".to_owned()),
        );

        let mut second_menage = Menage::new(2);
        second_menage
            .caracteristiques
            .insert(String::from("Age"), Caracteristique::Entier(30));

        second_menage.caracteristiques.insert(
            String::from("TypeLogenment"),
            Caracteristique::Textuel("Locataire".to_owned()),
        );

        let result = first_menage.compare_type_carac(&second_menage);
        assert_eq!(wanted, result);
    }
}
