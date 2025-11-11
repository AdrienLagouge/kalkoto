use crate::adapters::{MenageListAdapter, MenageListAdapterError};
use crate::entities::menage::{Caracteristique, Menage};
use crate::entities::menage_input::{MenageInput, MenageInputBuilder};
use crate::{KalkotoError, KalkotoResult};
use csv::{Reader, ReaderBuilder, StringRecord};
use std::ffi::OsStr;
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fs::{write, File},
    io::{self, Read},
    path::Path,
};

impl From<csv::Error> for MenageListAdapterError {
    fn from(e: csv::Error) -> MenageListAdapterError {
        MenageListAdapterError::Validation {
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
    pub fn populate_from_buf(
        self,
        input_buf: &[u8],
    ) -> KalkotoResult<(HashSet<String>, Vec<Menage>)> {
        let mut rdr = ReaderBuilder::new()
            .delimiter(b';')
            .has_headers(false)
            .from_reader(input_buf);

        let mut headers_row: Vec<String> = vec![];
        let mut vec_menage: Vec<Menage> = vec![];

        if let Some(result) = rdr.records().next() {
            let headers = result.map_err(|e| MenageListAdapterError::Validation {
                fault_index: -1,
                cause: "Problème à la lecture du header du CSV".to_string(),
                conseil: "Vérifier le fichier CSV".to_string(),
            })?;

            headers_row = headers
                .iter()
                .map(|str| str.to_string())
                .collect::<Vec<String>>();

            for (index, row) in rdr.records().enumerate() {
                let caracteristiques_vec: Vec<Caracteristique> = row
                    .map_err(|e| MenageListAdapterError::Validation {
                        fault_index: index as i32,
                        cause: "Les caractéristiques de ces ménages semblent invalides".to_string(),
                        conseil: "Vérifier le fichier CSV".to_string(),
                    })?
                    .iter()
                    .map(|str| str.to_string())
                    .map(Caracteristique::from)
                    .collect();

                let caracteristiques: HashMap<String, Caracteristique> = headers_row
                    .iter()
                    .cloned()
                    .zip(caracteristiques_vec.iter().cloned())
                    .collect();

                let menage = Menage {
                    index: (index as i32) + 1i32,
                    caracteristiques,
                };

                vec_menage.push(menage);
            }
        }

        let headers_set: HashSet<String> = headers_row.into_iter().collect();

        Ok((headers_set, vec_menage))
    }

    pub fn populate_from_path<P>(self, path: P, buf_string: &mut String) -> KalkotoResult<Self>
    where
        P: AsRef<Path>,
    {
        match path.as_ref().extension().and_then(OsStr::to_str) {
            Some("csv") => (),
            _ => {
                return Err(From::from(MenageListAdapterError::FileFormat(
                    "Le fichier indiqué n'est pas un CSV".into(),
                )))
            }
        }

        let mut f = match File::open(path) {
            Ok(file) => file,
            Err(e) => return Err(From::from(MenageListAdapterError::IO(e))),
        };

        // read the whole file
        let _ = match f.read_to_string(buf_string) {
            Ok(nbytes) => nbytes,
            Err(e) => return Err(From::from(MenageListAdapterError::IO(e))),
        };

        let input_slice = buf_string.as_bytes();

        let (set_caracteristiques, liste_menages) = self.populate_from_buf(input_slice)?;

        Ok(CsvInputAdapter {
            set_caracteristiques: Some(set_caracteristiques),
            liste_menages: Some(liste_menages),
        })
    }
}

impl MenageListAdapter for CsvInputAdapter {
    fn create_valid_menage_input(
        &self,
        empty_menage_input: MenageInputBuilder<super::EmptyList>,
    ) -> KalkotoResult<MenageInput> {
        match (
            self.set_caracteristiques.as_ref(),
            self.liste_menages.as_ref(),
        ) {
            (Some(set_caracteristiques), Some(liste_menages)) => empty_menage_input
                .from_unvalidated_liste_menage(liste_menages)
                .validate_liste_menage()?
                .build_valide_menage_input(),
            (_, _) => Err(From::from(MenageListAdapterError::Validation {
                fault_index: -1,
                cause:
                    "Impossible de construire une liste valide de ménages à partir de fichier CSV"
                        .to_string(),
                conseil: "Reprendre l'ordre des étapes de construction d'un input ménages"
                    .to_string(),
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempdir::TempDir;

    #[test]
    fn ok_csv_bytes_populate() -> KalkotoResult<()> {
        static VALID_CSV_BYTES: &[u8] = "Age;Revenu;TypeLogement\n35;500.5;Locataire".as_bytes();

        let mut wanted_hashset: HashSet<String> = HashSet::new();
        wanted_hashset.insert("Age".to_string());
        wanted_hashset.insert("Revenu".to_string());
        wanted_hashset.insert("TypeLogement".to_string());

        let mut wanted_hashmap: HashMap<String, Caracteristique> = HashMap::new();
        wanted_hashmap.insert("Age".to_string(), Caracteristique::Entier(35));
        wanted_hashmap.insert("Revenu".to_string(), Caracteristique::Numeric(500.5));
        wanted_hashmap.insert(
            "TypeLogement".to_string(),
            Caracteristique::Textuel("Locataire".to_string()),
        );

        let wanted_vec_menage = vec![Menage {
            index: 1,
            caracteristiques: wanted_hashmap,
        }];

        let (result_hashset, result_vec_menage) =
            CsvInputAdapter::new().populate_from_buf(VALID_CSV_BYTES)?;

        let wanted = true;
        let result = (wanted_hashset == result_hashset) && (wanted_vec_menage == result_vec_menage);

        assert_eq!(wanted, result);

        Ok(())
    }

    #[test]
    fn err_nosemicolon_headers_csv_bytes() -> KalkotoResult<()> {
        static UNVALID_CSV_BYTES: &[u8] = "\
        Age;Revenu,TypeLogement
        35;500.5;Locataire
        "
        .as_bytes();

        let result = CsvInputAdapter::new()
            .populate_from_buf(UNVALID_CSV_BYTES)
            .is_err();

        let wanted = true;

        assert_eq!(wanted, result);

        Ok(())
    }

    #[test]
    fn err_unequal_length_headerscsv_bytes() -> KalkotoResult<()> {
        static UNVALID_CSV_BYTES: &[u8] = "\
        Age;Revenu
        35;500.5;Locataire
        "
        .as_bytes();

        let result = CsvInputAdapter::new()
            .populate_from_buf(UNVALID_CSV_BYTES)
            .is_err();

        let wanted = true;

        assert_eq!(wanted, result);

        Ok(())
    }

    #[test]
    fn ok_csv_file_populate() -> KalkotoResult<()> {
        static VALID_CSV_BYTES: &[u8] = "Age;Revenu;TypeLogement\n35;500.5;Locataire".as_bytes();

        let tmp_dir = TempDir::new("test-input").map_err(MenageListAdapterError::IO)?;
        let file_path = tmp_dir.path().join("valid_csv.csv");
        let mut tmp_file = File::create(&file_path).map_err(MenageListAdapterError::IO)?;
        fs::write(&file_path, VALID_CSV_BYTES).map_err(MenageListAdapterError::IO)?;

        let mut wanted_hashset: HashSet<String> = HashSet::new();
        wanted_hashset.insert("Age".to_string());
        wanted_hashset.insert("Revenu".to_string());
        wanted_hashset.insert("TypeLogement".to_string());

        let mut wanted_hashmap: HashMap<String, Caracteristique> = HashMap::new();
        wanted_hashmap.insert("Age".to_string(), Caracteristique::Entier(35));
        wanted_hashmap.insert("Revenu".to_string(), Caracteristique::Numeric(500.5));
        wanted_hashmap.insert(
            "TypeLogement".to_string(),
            Caracteristique::Textuel("Locataire".to_string()),
        );

        let wanted_vec_menage = vec![Menage {
            index: 1,
            caracteristiques: wanted_hashmap,
        }];

        let mut csv_content = String::new();
        let CsvInputAdapter {
            set_caracteristiques: result_hashset,
            liste_menages: result_vec_menage,
        } = CsvInputAdapter::new().populate_from_path(file_path, &mut csv_content)?;

        let wanted = true;

        let result = (wanted_hashset == result_hashset.unwrap())
            && (wanted_vec_menage == result_vec_menage.unwrap());
        assert_eq!(wanted, result);

        drop(tmp_file);
        tmp_dir.close().map_err(MenageListAdapterError::IO)?;

        Ok(())
    }

    #[test]
    fn err_csv_not_valid_file_path() -> KalkotoResult<()> {
        let mut csv_content = String::new();
        let result = CsvInputAdapter::new()
            .populate_from_path("nonexistent_file.csv", &mut csv_content)
            .is_err();

        let wanted = true;

        assert_eq!(wanted, result);

        Ok(())
    }

    #[test]
    fn err_csv_file_invalid_row() -> KalkotoResult<()> {
        static UNVALID_CSV_CONTENT: &str = "\
        Age;Revenu;TypeLogement
        35,500.5;Locataire
        ";

        let tmp_dir = TempDir::new("test-input").map_err(MenageListAdapterError::IO)?;
        let file_path = tmp_dir.path().join("valid_csv.csv");
        let mut tmp_file = File::create(&file_path).map_err(MenageListAdapterError::IO)?;
        fs::write(&file_path, UNVALID_CSV_CONTENT).map_err(MenageListAdapterError::IO)?;

        let mut csv_content = String::new();
        let result = CsvInputAdapter::new()
            .populate_from_path(file_path, &mut csv_content)
            .is_err();

        let wanted = true;

        assert_eq!(wanted, result);

        Ok(())
    }
}
