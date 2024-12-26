use anyhow::{anyhow, Result};
use kalkoto_lib::adapters::*;
use kalkoto_lib::entities::menage::*;

fn main() -> Result<()> {
    let mut menage_1 = Menage::new(1);
    let mut menage_2 = Menage::new(2);
    let mut menage_3 = Menage::new(3);
    menage_1
        .caracteristiques
        .insert("Age".to_string(), Caracteristique::Entier(25));
    menage_2
        .caracteristiques
        .insert("Age".to_string(), Caracteristique::Entier(35));
    menage_3
        .caracteristiques
        .insert("Age".to_string(), Caracteristique::Numeric(40.0f64));
    let valid_vec = vec![menage_1, menage_2, menage_3];
    let test_input = MenageInputBuilder::<EmptyList>::new()
        .from_unvalidated_liste_menage(valid_vec)
        .validate_liste_menage()?;
    Ok(())
}
