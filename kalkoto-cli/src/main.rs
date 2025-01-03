use anyhow::Result;
use kalkoto_lib::adapters::csv_input_adapter::*;
use kalkoto_lib::adapters::*;
use std::collections::HashMap;
use toml_input_adapter::TomlInputAdapter;

fn main() -> Result<()> {
    let csv_input_adapter = CsvInputAdapter::new();
    let mut csv_content = String::new();
    let potential_csv_input_adapter =
        csv_input_adapter.populate_from_path("../test-input/good_input.csv", &mut csv_content)?;
    let menage_input = MenageInputBuilder::<EmptyList>::new();
    let menage_input = potential_csv_input_adapter.create_valid_menage_input(menage_input)?;
    //let (valid_carac_set, valid_liste_menage) = menage_input.get_valid_input_menages();
    //println!("Headers extraits du fichier : {:?}", valid_carac_set);
    println!("Menages extraits du fichier CSV: {}", menage_input);

    let toml_input_adapter = TomlInputAdapter::new("../test-input/good_input.toml");
    let policy_input = &toml_input_adapter.create_valid_policy_input()?;
    println!(
        "Politique publique extraite du fichier TOML : {}",
        policy_input
    );

    let composante_1 = &policy_input.valid_policy.composantes_ordonnees[0];
    let composante_2 = &policy_input.valid_policy.composantes_ordonnees[1];
    let menage_1 = menage_input.liste_menage_valide[0].clone();
    let parameters_dict = policy_input.valid_policy.parameters_values.clone();
    let variables_dict = HashMap::new();
    let variables_dict =
        composante_1.simulate_menage(&menage_1, variables_dict, &parameters_dict)?;
    let _ = composante_2.simulate_menage(&menage_1, variables_dict, &parameters_dict)?;
    Ok(())
}
