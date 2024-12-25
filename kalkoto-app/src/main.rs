#![allow(non_snake_case)]
// Import the Dioxus prelude to gain access to the `rsx!` macro and the `Scope` and `Element` types.
use dioxus::prelude::*;
use kalkoto_lib::entities::menage::{Caracteristique, Menage};

fn main() {
    // Launch the web application using the App component as the root.
    dioxus::launch(App);
}

// Define a component that renders a div with constant text
#[component]
fn App() -> Element {
    let mut test_menage = Menage::new(1);
    test_menage.caracteristiques.insert(
        "Name".to_owned(),
        Caracteristique::Textuel("Jeannot".to_owned()),
    );
    test_menage
        .caracteristiques
        .insert("Age".to_owned(), Caracteristique::Entier(35));
    rsx! {
        div {
            "Coucou de mon appli Simulaction Sociale !"
        }
    TestComponentWithProps {menage: test_menage}
    }
}

#[component]
fn TestComponentWithProps(menage: Menage) -> Element {
    rsx! {
        "Exposition des entités de la librairie. Exemple :\n {menage}"
    }
}
