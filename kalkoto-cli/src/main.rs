use clap::Parser;
use crossterm::style::Stylize;
use kalkoto_lib::adapters::input_adapters::arrow_input_adapter::ArrowInputAdapter;
use kalkoto_lib::adapters::input_adapters::*;
use kalkoto_lib::adapters::output_adapters::arrow_output_adapter::ArrowOutputAdapter;
use kalkoto_lib::adapters::output_adapters::csv_output_adapter::CSVOutputAdapter;
use kalkoto_lib::adapters::output_adapters::{OutputAdapter, OutputWriter};
use kalkoto_lib::entities::simulator::{
    EmptyBaselineInput, EmptyMenageInput, EmptyVarianteInput, SimulatorBuilder,
};
use kalkoto_lib::KalkotoResult;
use std::path::Path;
use toml_input_adapter::TomlInputAdapter;

#[derive(Parser)]
#[command(author,version,about,long_about = None)]
struct Args {
    #[arg(short, long, value_name = "Type du fichier ménages (csv ou arrow)")]
    type_menage_input: String,

    #[arg(short, long, value_name = "Chemin vers le fichier ménages")]
    menage_input: String,

    #[arg(
        short,
        long,
        value_name = "Chemin vers le fichier TOML de la politique publique de référence"
    )]
    baseline_policy_input: String,

    #[arg(
        short,
        long,
        value_name = "Chemin vers le fichier TOML de la politique publique de variante"
    )]
    variante_policy_input: Option<String>,

    #[arg(short, long, value_name = "Préfixe pour les fichiers de sortie")]
    prefix: Option<String>,
}

struct Adapters<I, O>
where
    I: MenageListCreator,
    O: OutputWriter,
{
    input_adapter: I,
    output_adapter: O,
}

fn dispatch_adapters<P: AsRef<Path>>(
    type_fichier_menages: &str,
    menage_input_path: P,
    prefix: &Option<String>,
) -> KalkotoResult<Adapters<MenageAdapter, OutputAdapter>> {
    match type_fichier_menages {
        "arrow" => {
            let input_adapter = ArrowInputAdapter::new().populate_from_path(menage_input_path)?;
            let mut output_adapter = ArrowOutputAdapter::new();
            if let Some(prefix) = prefix.as_deref() {
                output_adapter = output_adapter.add_output_prefix(prefix.to_string())
            };
            Ok(Adapters {
                input_adapter: MenageAdapter::Arrow(input_adapter),
                output_adapter: OutputAdapter::Arrow(output_adapter),
            })
        }
        "csv" => {
            let mut csv_empty_buf = String::new();
            let input_adapter = csv_input_adapter::CsvInputAdapter::new()
                .populate_from_path(menage_input_path, &mut csv_empty_buf)?;
            let mut output_adapter = CSVOutputAdapter::new();
            if let Some(prefix) = prefix.as_deref() {
                output_adapter = output_adapter.add_output_prefix(prefix.to_string())
            }
            Ok(Adapters {
                input_adapter: MenageAdapter::CSV(input_adapter),
                output_adapter: OutputAdapter::CSV(output_adapter),
            })
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

    let Adapters {
        input_adapter,
        output_adapter,
    } = dispatch_adapters(&args.type_menage_input, menage_input_path, &args.prefix)?;
    // let menage_input_adapter = dispatch_input_adapter(menage_input_path)?;

    let sim_builder =
        SimulatorBuilder::<EmptyMenageInput, EmptyBaselineInput, EmptyVarianteInput>::new();

    println!(
        "{}",
        "1) Import des informations du fichier ménages"
            .yellow()
            .bold()
            .underlined()
    );

    let sim_builder = sim_builder.add_menage_input(input_adapter)?;

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

    sim_builder.export_baseline(&output_adapter)?;

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

        sim_builder.export_variante_and_diff(output_adapter)?;
    }

    Ok(())
}
