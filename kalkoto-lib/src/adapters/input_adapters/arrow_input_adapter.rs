use crate::adapters::{MenageListAdapter, MenageListAdapterError};
use crate::entities::menage::{self, Caracteristique, Menage};
use crate::entities::menage_input::{MenageInput, MenageInputBuilder};
use crate::{KalkotoError, KalkotoResult};
use arrow::array::{Array, DataTypeLayout, Float64Array, Int32Array, StringArray};
use arrow::ipc::reader::FileReader;
use memmap2::Mmap;
use std::collections::HashSet;
use std::ffi::{FromVecWithNulError, OsStr};
use std::vec;
use std::{fs::File, path::Path, sync::Arc};

#[derive(Default, Debug)]
pub struct ArrowInputAdapter {
    dataframe: Option<Vec<(String, Vec<Caracteristique>)>>,
    ncol: usize,
    nrow: usize,
}

impl ArrowInputAdapter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn populate_from_path<P>(self, path: P) -> KalkotoResult<Self>
    where
        P: AsRef<Path>,
    {
        match path.as_ref().extension().and_then(OsStr::to_str) {
            Some("arrow") => (),
            _ => {
                return Err(From::from(MenageListAdapterError::FileFormat(
                    "Le fichier indiqué n'est pas un Arrow dataframe".into(),
                )))
            }
        }

        let mut file = match File::open(path) {
            Ok(file) => file,
            Err(e) => return Err(From::from(MenageListAdapterError::IO(e))),
        };
        let mmap = unsafe { Mmap::map(&file).map_err(MenageListAdapterError::IO)? };

        let mut reader = FileReader::try_new(std::io::Cursor::new(mmap), None)
            .map_err(MenageListAdapterError::Arrow)?;

        let extracted_dataframe: KalkotoResult<Self> = match reader.next() {
            Some(batch) => {
                let batch = batch.map_err(MenageListAdapterError::Arrow)?;
                let (schema, columns, nrow) = batch.into_parts();

                let ncol = columns.len();

                let column_names: Vec<String> = schema
                    .fields
                    .iter()
                    .map(|field| field.name().to_owned())
                    .collect();

                let column_values: KalkotoResult<Vec<Vec<Caracteristique>>> = columns
                    .into_iter()
                    .map(|column| extract_values_from_arrow(column.clone(), nrow))
                    .collect();

                let column_values = column_values?;

                let column_extract =
                    column_names
                        .into_iter()
                        .zip(column_values)
                        .collect::<Vec<(String, Vec<Caracteristique>)>>();

                Ok(Self {
                    dataframe: Some(column_extract),
                    ncol,
                    nrow,
                })
            }
            _ => Err(KalkotoError::ListMenageError(
                MenageListAdapterError::Arrow(arrow::error::ArrowError::ParseError(
                    "Erreur à l'extraction des colonnes du Arrow Dataframe".into(),
                )),
            )),
        };

        extracted_dataframe
    }
}

pub fn extract_values_from_arrow(
    array: Arc<dyn Array>,
    nrow: usize,
) -> KalkotoResult<Vec<Caracteristique>> {
    match array.data_type() {
        // Cas 1 : Entiers (Int32)
        arrow::datatypes::DataType::Int32 => {
            let int_array = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                KalkotoError::ListMenageError(MenageListAdapterError::Arrow(
                    arrow::error::ArrowError::CastError(
                        "Erreur à la lecture d'une colonne de type Int32".into(),
                    ),
                ))
            })?;

            let values: KalkotoResult<Vec<Caracteristique>> = (0..nrow)
                .map(|i| {
                    if int_array.is_null(i) {
                        Err(KalkotoError::ListMenageError(
                            MenageListAdapterError::Validation {
                                fault_index: i as i32,
                                cause: "Valeur manquante (NA) !".into(),
                                conseil: "Vérifier la construction du dataframe R".into(),
                            },
                        ))
                    } else {
                        Ok(Caracteristique::Entier(int_array.value(i)))
                    }
                })
                .collect();

            values
        }
        // Cas 2 : Flottants (Float64)
        arrow::datatypes::DataType::Float64 => {
            let float_array = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    KalkotoError::ListMenageError(MenageListAdapterError::Arrow(
                        arrow::error::ArrowError::CastError(
                            "Erreur à la lecture d'une colonne de type Float64".into(),
                        ),
                    ))
                })?;

            let values: KalkotoResult<Vec<Caracteristique>> = (0..nrow)
                .map(|i| {
                    if float_array.is_null(i) {
                        Err(KalkotoError::ListMenageError(
                            MenageListAdapterError::Validation {
                                fault_index: i as i32,
                                cause: "Valeur manquante (NA) !".into(),
                                conseil: "Vérifier la construction du dataframe R".into(),
                            },
                        ))
                    } else {
                        Ok(Caracteristique::Numeric(float_array.value(i)))
                    }
                })
                .collect();

            values
        }
        // Cas 3 : Chaînes de caractères
        arrow::datatypes::DataType::Utf8 => {
            let str_array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    KalkotoError::ListMenageError(MenageListAdapterError::Arrow(
                        arrow::error::ArrowError::CastError(
                            "Erreur à la lecture d'une colonne de type Utf8/String".into(),
                        ),
                    ))
                })?;

            let values: KalkotoResult<Vec<Caracteristique>> = (0..nrow)
                .map(|i| {
                    if str_array.is_null(i) {
                        Err(KalkotoError::ListMenageError(
                            MenageListAdapterError::Validation {
                                fault_index: i as i32,
                                cause: "Valeur manquante (NA) !".into(),
                                conseil: "Vérifier la construction du dataframe R".into(),
                            },
                        ))
                    } else {
                        Ok(Caracteristique::Textuel(str_array.value(i).to_string()))
                    }
                })
                .collect();

            values
        }
        // Les autres types Arrow ne sont pas reconnus comme valides
        _ => Err(KalkotoError::ListMenageError(
            MenageListAdapterError::Arrow(arrow::error::ArrowError::SchemaError(
                "Le type de la colonne n'est pas supporté".into(),
            )),
        )),
    }
}

impl MenageListAdapter for ArrowInputAdapter {
    fn create_valid_menage_input(
        self,
        empty_menage_input: MenageInputBuilder<crate::entities::menage_input::EmptyList>,
    ) -> KalkotoResult<MenageInput> {
        match self.dataframe {
            Some(dataframe) => {
                let mut liste_menages = vec![Menage::new(0); self.nrow];
                //(Some(set_caracteristiques), Some(liste_menages)) =
                for i in (0..self.nrow) {
                    let mut menage = Menage::new(i as i32);
                    for (nom_caracteristique, valeurs_caracteristique) in
                        dataframe.iter().take(self.ncol)
                    {
                        menage.caracteristiques.insert(
                            nom_caracteristique.clone(),
                            valeurs_caracteristique
                                .get(i)
                                .ok_or_else(|| MenageListAdapterError::Validation {
                                    fault_index: i as i32,
                                    cause: format!(
                                        "Problème à la lecture de la composante {}",
                                        nom_caracteristique
                                    ),
                                    conseil: "".into(),
                                })?
                                .clone(),
                        );
                        if let Some(menage_init) = liste_menages.get_mut(i) {
                            *menage_init = menage.clone();
                        }
                    }
                }

                empty_menage_input
                    .from_unvalidated_liste_menage(&liste_menages)
                    .validate_liste_menage()?
                    .build_valide_menage_input()
            }
            _ => Err(KalkotoError::ListMenageError(
                MenageListAdapterError::Uninitialized,
            )),
        }
    }
}
