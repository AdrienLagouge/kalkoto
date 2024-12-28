use crate::adapters::{MenageListAdapter, MenageListAdapterError};
use crate::entities::menage::Menage;
use csv::{Reader, ReaderBuilder, StringRecord};
use std::collections::HashSet;
use std::error::Error;
use std::{
    fs::{write, File},
    io::{self, Read},
    path::Path,
};

use super::{KalkotoError, KalkotoResult};

impl From<csv::Error> for MenageListAdapterError {
    fn from(e: csv::Error) -> MenageListAdapterError {
        MenageListAdapterError::ValidationError {
            fault_index: 0,
            cause: "Problème à la lecture du header et/ou du contenu du CSV".to_string(),
            conseil: "Vérifier le fichier CSV".to_string(),
        }
    }
}

#[derive(Debug, Default)]
pub struct CsvInputAdapter {
    set_caracteristiques: Option<HashSet<String>>,
    liste_menages: Option<Vec<Menage>>,
}

impl CsvInputAdapter {
    pub fn new() -> Self {
        CsvInputAdapter::default()
    }
}

impl CsvInputAdapter {
    pub fn extract_headers_from_buf(&self, input_buf: &[u8]) -> KalkotoResult<HashSet<String>> {
        let mut rdr = ReaderBuilder::new()
            .delimiter(b';')
            .has_headers(false)
            .from_reader(input_buf);

        let mut headers_row: HashSet<String> = HashSet::new();
        {
            if let Some(result) = rdr.records().next() {
                let headers = result.map_err(MenageListAdapterError::from)?;
                let first_row = rdr
                    .records()
                    .next()
                    .unwrap_or(Ok(StringRecord::new()))
                    .map_err(MenageListAdapterError::from)?;

                headers_row = headers
                    .iter()
                    .map(|str| str.to_string())
                    .collect::<HashSet<String>>();
            } else {
                return Err(From::from(MenageListAdapterError::ValidationError {
                    fault_index: -1,
                    cause: "Problème à la lecture du header du CSV".to_string(),
                    conseil: "Vérifier le fichier CSV".to_string(),
                }));
            };
        }
        Ok(headers_row)
    }

    pub fn extract_headers_from_path<P>(
        &self,
        path: P,
        buf_string: &mut String,
    ) -> KalkotoResult<HashSet<String>>
    where
        P: AsRef<Path>,
    {
        let mut f = match File::open(path) {
            Ok(file) => file,
            Err(e) => return Err(From::from(MenageListAdapterError::IO(e))),
        };

        // read the whole file
        let _ = match f.read_to_string(buf_string) {
            Ok(nbytes) => nbytes,
            Err(e) => return Err(From::from(MenageListAdapterError::IO(e))),
        };

        let output_slice = buf_string.as_bytes();

        self.extract_headers_from_buf(output_slice)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempdir::TempDir;

    #[test]
    fn ok_csv_bytes_extract_headers() -> KalkotoResult<()> {
        static VALID_CSV_BYTES: &[u8] = "\
        Age;Revenu;TypeLogement
        35;500.5;Locataire
        "
        .as_bytes();

        let mut wanted_hashset: HashSet<String> = HashSet::new();
        wanted_hashset.insert("Age".to_string());
        wanted_hashset.insert("Revenu".to_string());
        wanted_hashset.insert("TypeLogement".to_string());

        let result_hashset = CsvInputAdapter::new().extract_headers_from_buf(VALID_CSV_BYTES)?;

        let wanted = true;
        let result = wanted_hashset == result_hashset;

        assert_eq!(wanted, result);

        Ok(())
    }

    #[test]
    fn err_nosemicolon_csv_bytes_extract_headers() -> KalkotoResult<()> {
        static UNVALID_CSV_BYTES: &[u8] = "\
        Age;Revenu,TypeLogement
        35;500.5;Locataire
        "
        .as_bytes();

        let result = CsvInputAdapter::new()
            .extract_headers_from_buf(UNVALID_CSV_BYTES)
            .is_err();

        let wanted = true;

        assert_eq!(wanted, result);

        Ok(())
    }

    #[test]
    fn err_unequal_length_csv_bytes_extract_headers() -> KalkotoResult<()> {
        static UNVALID_CSV_BYTES: &[u8] = "\
        Age;Revenu
        35;500.5;Locataire
        "
        .as_bytes();

        let result = CsvInputAdapter::new()
            .extract_headers_from_buf(UNVALID_CSV_BYTES)
            .is_err();

        let wanted = true;

        assert_eq!(wanted, result);

        Ok(())
    }

    #[test]
    fn ok_csv_file_extract_headers() -> KalkotoResult<()> {
        static VALID_CSV_CONTENT: &str = "\
        Age;Revenu;TypeLogement
        35;500.5;Locataire
        ";

        let tmp_dir = TempDir::new("test-input").map_err(MenageListAdapterError::IO)?;
        let file_path = tmp_dir.path().join("valid_csv.csv");
        let mut tmp_file = File::create(&file_path).map_err(MenageListAdapterError::IO)?;
        fs::write(&file_path, VALID_CSV_CONTENT).map_err(MenageListAdapterError::IO)?;

        let mut wanted_hashset: HashSet<String> = HashSet::new();
        wanted_hashset.insert("Age".to_string());
        wanted_hashset.insert("Revenu".to_string());
        wanted_hashset.insert("TypeLogement".to_string());

        let mut csv_content = String::new();
        let result_hashset =
            CsvInputAdapter::new().extract_headers_from_path(file_path, &mut csv_content)?;

        let wanted = true;
        let result = wanted_hashset == result_hashset;

        assert_eq!(wanted, result);

        drop(tmp_file);
        tmp_dir.close().map_err(MenageListAdapterError::IO)?;

        Ok(())
    }

    #[test]
    fn err_csv_not_valid_file_extract_headers() -> KalkotoResult<()> {
        let mut csv_content = String::new();
        let result = CsvInputAdapter::new()
            .extract_headers_from_path("nonexistent_file.csv", &mut csv_content)
            .is_err();

        let wanted = true;

        assert_eq!(wanted, result);

        Ok(())
    }
}
