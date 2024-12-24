#![allow(non_snake_case)]
// Import the Dioxus prelude to gain access to the `rsx!` macro and the `Scope` and `Element` types.
use dioxus::prelude::*;
use kalkoto_lib::greeter;

fn main() {
    // Launch the web application using the App component as the root.
    dioxus::launch(App);
}

// Define a component that renders a div with constant text
#[component]
fn App() -> Element {
    let libtext = greeter("TestComponent");
    rsx! {
        div {
            "Coucou de mon appli Simulaction Sociale !"
        }
    TestComponentWithProps {name: libtext}
    }
}

#[component]
fn TestComponentWithProps(name: String) -> Element {
    rsx! {
        "Texte issu de la librairie. {name}"
    }
}
