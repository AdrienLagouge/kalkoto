use std::ffi::OsStr;
use std::path::Path;

use clap::Parser;
use crossterm::style::Stylize;
use kalkoto_lib::adapters::input_adapters::arrow_input_adapter::ArrowInputAdapter;
use kalkoto_lib::adapters::input_adapters::*;
use kalkoto_lib::adapters::output_adapters::csv_output_adapter::CSVOutputAdapter;
use kalkoto_lib::entities::simulator::{
    EmptyBaselineInput, EmptyMenageInput, EmptyVarianteInput, SimulatorBuilder,
};
use kalkoto_lib::KalkotoResult;
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

fn dispatch_input_adapter<P: AsRef<Path>>(
    menage_input_path: P,
) -> KalkotoResult<impl MenageListAdapter> {
    match menage_input_path
        .as_ref()
        .extension()
        .and_then(OsStr::to_str)
    {
        Some("arrow") => Ok(MenageAdapter::ArrowAdapter(
            ArrowInputAdapter::new().populate_from_path(menage_input_path)?,
        )),
        Some("csv") => {
            let mut csv_empty_buf = String::new();
            Ok(MenageAdapter::CSVAdapter(
                csv_input_adapter::CsvInputAdapter::new()
                    .populate_from_path(menage_input_path, &mut csv_empty_buf)?,
            ))
        }
        _ => Err(MenageListAdapterError::FileFormat(
            "Le fichier indiqué n'est pas un Arrow dataframe".into(),
        )
        .into()),
    }
}

fn main() -> KalkotoResult<()> {
    let args = Args::parse();

    let menage_input_path = Path::new(&args.menage_input);

    let menage_input_adapter = dispatch_input_adapter(menage_input_path)?;

    let mut csv_writer = CSVOutputAdapter::new();

    if let Some(prefix) = args.prefix.as_deref() {
        csv_writer = csv_writer.add_output_prefix(prefix.to_string())
    }

    let sim_builder =
        SimulatorBuilder::<EmptyMenageInput, EmptyBaselineInput, EmptyVarianteInput>::new();

    println!(
        "{}",
        "1) Import des informations du fichier ménages"
            .yellow()
            .bold()
            .underlined()
    );
    println!("Debug {:?}", &args.menage_input);

    let sim_builder = sim_builder.add_menage_input(menage_input_adapter)?;

    println!("{}", &sim_builder.menage_input.0);

    println!(
        "{}",
        "2) Import des informations du fichier de référence de politique publique + simulation"
            .yellow()
            .bold()
            .underlined()
    );

    let mut baseline_empty_buf = String::new();

    let toml_input_adapter_baseline = TomlInputAdapter::new()
        .populate_from_path(&args.baseline_policy_input, &mut baseline_empty_buf)?;

    let mut sim_builder = sim_builder.add_valid_baseline_policy(toml_input_adapter_baseline)?;

    println!("{}", &sim_builder.policy_baseline.0);

    sim_builder.simulate_baseline_policy()?;

    println!(
        "{}",
        "Export des résultats de la simulation baseline\n"
            .blue()
            .bold()
    );

    sim_builder.export_baseline(&csv_writer)?;

    if let Some(variante_input) = args.variante_policy_input {
        println!(
            "{}",
            "3) Import des informations du fichier de variante de politique publique + simulation"
                .yellow()
                .bold()
                .underlined()
        );

        let mut variante_empty_buf = String::new();

        let toml_input_adapter_variante =
            TomlInputAdapter::new().populate_from_path(&variante_input, &mut variante_empty_buf)?;

        let mut sim_builder = sim_builder.add_valid_variante_policy(toml_input_adapter_variante)?;

        println!("{}", &sim_builder.policy_variante.0);

        sim_builder.simulate_variante_policy()?;

        println!(
            "{}",
            "Export des résultats de la simulation variante\n"
                .blue()
                .bold()
        );

        sim_builder.export_variante(&csv_writer)?;
    }

    Ok(())
}
