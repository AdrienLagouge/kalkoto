use anyhow::Result;
use kalkoto_lib::adapters::csv_input_adapter::*;
use kalkoto_lib::adapters::*;
use kalkoto_lib::entities::simulator::{
    EmptyBaselineInput, EmptyMenageInput, EmptyVarianteInput, SimulatorBuilder,
};

use toml_input_adapter::TomlInputAdapter;

fn main() -> Result<()> {
    let sim_builder =
        SimulatorBuilder::<EmptyMenageInput, EmptyBaselineInput, EmptyVarianteInput>::new();

    let sim_builder = sim_builder.add_output_prefix("test".to_string());

    let mut csv_input_adapter = CsvInputAdapter::new();
    let mut csv_content = String::new();
    csv_input_adapter =
        csv_input_adapter.populate_from_path("../test-input/good_input.csv", &mut csv_content)?;

    let sim_builder = sim_builder.add_menage_input(&csv_input_adapter)?;
    println!(
        "Menages extraits du fichier CSV: {}",
        sim_builder.menage_input.0
    );

    let toml_input_adapter = TomlInputAdapter::new("../test-input/good_input.toml");

    let sim_builder = sim_builder.add_valid_baseline_policy(&toml_input_adapter)?;

    println!(
        "Politique publique extraite du fichier TOML : {}",
        sim_builder.policy_baseline.0
    );

    let sim_builder = sim_builder.simulate_baseline_policy()?;

    println!(
        "->>>> Debug results : {:?}",
        sim_builder.results_baseline.unwrap()
    );
    Ok(())
}
