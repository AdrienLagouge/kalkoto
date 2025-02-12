use anyhow::Result;
use clap::Parser;
use crossterm::style::Stylize;
use kalkoto_lib::adapters::csv_input_adapter::*;
use kalkoto_lib::adapters::*;
use kalkoto_lib::entities::simulator::{
    EmptyBaselineInput, EmptyMenageInput, EmptyVarianteInput, SimulatorBuilder,
};
use toml_input_adapter::TomlInputAdapter;

#[derive(Parser)]
#[command(author,version,about,long_about = None)]
struct Args {
    #[arg(short, long, value_name = "Fichier ménages")]
    menage_input: String,

    #[arg(short, long, value_name = "Fichier politique publique de référence")]
    baseline_policy_input: String,

    #[arg(short, long, value_name = "Fichier politique publique de variante")]
    variante_policy_input: Option<String>,

    #[arg(short, long, value_name = "Préfixe pour les fichiers de sortie")]
    prefix: Option<String>,
}

fn main() -> Result<()> {
    let mut sim_builder =
        SimulatorBuilder::<EmptyMenageInput, EmptyBaselineInput, EmptyVarianteInput>::new();

    let args = Args::parse();

    if let Some(prefix) = args.prefix.as_deref() {
        sim_builder = sim_builder.add_output_prefix(prefix.to_string())
    }

    println!(
        "{}",
        "1) Import des informations du fichier ménages"
            .yellow()
            .bold()
            .underlined()
    );

    let mut csv_input_adapter = CsvInputAdapter::new();
    let mut csv_content = String::new();
    csv_input_adapter =
        csv_input_adapter.populate_from_path(&args.menage_input, &mut csv_content)?;

    let sim_builder = sim_builder.add_menage_input(&csv_input_adapter)?;

    println!("{}", &sim_builder.menage_input.0);

    println!(
        "{}",
        "2) Import des informations du fichier de référence de politique publique + simulation"
            .yellow()
            .bold()
            .underlined()
    );

    let toml_input_adapter_baseline = TomlInputAdapter::new(&args.baseline_policy_input);

    let mut sim_builder = sim_builder.add_valid_baseline_policy(&toml_input_adapter_baseline)?;

    println!("{}", &sim_builder.policy_baseline.0);

    sim_builder.simulate_baseline_policy()?;

    println!(
        "{}",
        "Export des résultats de la simulation baseline\n"
            .blue()
            .bold()
    );

    sim_builder.export_baseline_results_csv()?;

    if let Some(variante_input) = args.variante_policy_input {
        println!(
            "{}",
            "3) Import des informations du fichier de variante de politique publique + simulation"
                .yellow()
                .bold()
                .underlined()
        );
        let toml_input_adapter_variante = TomlInputAdapter::new(&variante_input);

        let mut sim_builder =
            sim_builder.add_valid_variante_policy(&toml_input_adapter_variante)?;

        println!("{}", &sim_builder.policy_variante.0);

        sim_builder.simulate_variante_policy()?;

        println!(
            "{}",
            "Export des résultats de la simulation variante\n"
                .blue()
                .bold()
        );

        sim_builder.export_variante_results_csv()?;
    }

    Ok(())
}
