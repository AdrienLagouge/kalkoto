use anyhow::Result;
use kalkoto_lib::adapters::csv_input_adapter::*;

fn main() -> Result<()> {
    let csv_input_adapter = CsvInputAdapter::new();
    let mut csv_content = String::new();
    let headers = csv_input_adapter.extract_headers_from_path(
        "../test-input/bad_input_headers_unequal_length.csv",
        &mut csv_content,
    )?;
    println!("Headers extraits du fichier : {:?}", headers);
    println!("Contenu extrait du fichier : {:?}", csv_content);
    Ok(())
}
