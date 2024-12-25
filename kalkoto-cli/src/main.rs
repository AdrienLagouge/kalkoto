use kalkoto_lib::entities::menage::{Caracteristique, Menage};

fn main() {
    let mut my_menage = Menage::new(1);
    my_menage
        .caracteristiques
        .insert(String::from("Age"), Caracteristique::Entier(30));

    my_menage.caracteristiques.insert(
        String::from("TypeLogenment"),
        Caracteristique::Textuel("Locataire".to_owned()),
    );

    my_menage
        .caracteristiques
        .insert(String::from("Revenu"), Caracteristique::Numeric(500.65f64));

    println!("{}", my_menage);
    println!("{:?}", my_menage);
}
