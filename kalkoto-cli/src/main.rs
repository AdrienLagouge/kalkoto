use anyhow::Result;
use kalkoto_lib::adapters::csv_input_adapter::*;
use kalkoto_lib::adapters::*;

fn main() -> Result<()> {
    let csv_input_adapter = CsvInputAdapter::new();
    let mut csv_content = String::new();
    let potential_csv_input_adapter =
        csv_input_adapter.populate_from_path("../test-input/good_input.csv", &mut csv_content)?;
    let menage_input = MenageInputBuilder::<EmptyList>::new();
    let menage_input = potential_csv_input_adapter.create_valid_menage_input(menage_input)?;
    let (valid_carac_set, valid_liste_menage) = menage_input.get_valid_input_menages();
    println!("Headers extraits du fichier : {:?}", valid_carac_set);
    println!("Menages extraits du fichier : {:?}", valid_liste_menage);
    Ok(())
}
