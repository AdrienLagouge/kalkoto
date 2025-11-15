use crate::{
    adapters::output_adapters::{OutputAdapterError, OutputWriter},
    entities::menage::{Caracteristique, Menage},
    entities::simulator::{
        SimulationError, SimulatorBuilder, ValidBaselineInput, ValidMenageInput, ValidVarianteInput,
    },
    KalkotoError, KalkotoResult,
};
use arrow::array::{Array, ArrayRef, Float64Array, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_ipc::writer::FileWriter;
use itertools::Itertools;
use std::{collections::HashMap, fs::File, path::Path, sync::Arc};

#[derive(Default)]
pub struct ArrowOutputAdapter {
    output_prefix: Option<String>,
}

// Trait personnalisé
trait AllowedValue: private::Sealed {}
mod private {
    pub trait Sealed {}
    impl Sealed for f64 {}
    impl Sealed for Option<f64> {}
}
impl AllowedValue for f64 {}
impl AllowedValue for Option<f64> {}

impl ArrowOutputAdapter {
    pub fn new() -> Self {
        ArrowOutputAdapter::default()
    }

    pub fn add_output_prefix(&mut self, prefix: String) -> Self {
        Self {
            output_prefix: Some(prefix.clone()),
        }
    }
}

impl OutputWriter for ArrowOutputAdapter {
    fn export_baseline_results<E>(
        &self,
        simulated: &SimulatorBuilder<ValidMenageInput, ValidBaselineInput, E>,
    ) -> KalkotoResult<()> {
        match simulated.results_baseline {
            Some(ref results_baseline) => {
                let output_path = match &self.output_prefix {
                    Some(output_prefix) => format!("{}-baseline-results.arrow", output_prefix),
                    _ => String::from("baseline-results.arrow"),
                };

                let record_menage = create_record_batch_from_menage_list(
                    &simulated.menage_input.0.liste_menage_valide,
                )?;

                let record_baseline_results =
                    create_record_batch_from_list_dict_results(results_baseline)?;

                let final_record =
                    create_final_record_batch(&record_menage, &record_baseline_results)?;

                write_final_record(&final_record, output_path)
            }
            None => Err(KalkotoError::from(OutputAdapterError::Custom(
                "La simulation n'a pas encore été réalisée !".into(),
            ))),
        }
    }

    fn export_variante_results(
        &self,
        simulated: &SimulatorBuilder<ValidMenageInput, ValidBaselineInput, ValidVarianteInput>,
    ) -> KalkotoResult<()> {
        let export_variante = match simulated.results_variante {
            Some(ref results_variante) => {
                let output_path = match &self.output_prefix {
                    Some(output_prefix) => format!("{}-variante-results.arrow", output_prefix),
                    _ => String::from("variante-results.arrow"),
                };

                let record_menage = create_record_batch_from_menage_list(
                    &simulated.menage_input.0.liste_menage_valide,
                )?;

                let record_variante_results =
                    create_record_batch_from_list_dict_results(results_variante)?;

                let final_record =
                    create_final_record_batch(&record_menage, &record_variante_results)?;

                write_final_record(&final_record, output_path)
            }
            None => Err(KalkotoError::from(OutputAdapterError::Custom(
                "La simulation n'a pas encore été réalisée !".into(),
            ))),
        };
        export_variante?;

        let export_diff = match simulated.results_diff {
            Some(ref results_diff) => {
                let output_path = match &self.output_prefix {
                    Some(output_prefix) => format!("{}-diff-results.arrow", output_prefix),
                    _ => String::from("diff-results.arrow"),
                };

                let record_menage = create_record_batch_from_menage_list(
                    &simulated.menage_input.0.liste_menage_valide,
                )?;

                let record_diff_results = create_record_batch_from_list_dict_results(results_diff)?;

                let final_record = create_final_record_batch(&record_menage, &record_diff_results)?;

                write_final_record(&final_record, output_path)
            }
            None => Err(KalkotoError::from(OutputAdapterError::Custom(
                "La simulation n'a pas encore été réalisée !".into(),
            ))),
        };
        export_diff
    }
}

fn create_field_from_caracteristique(
    nom_caracteristique: &str,
    caracteristique: &Caracteristique,
) -> Field {
    match caracteristique {
        Caracteristique::Entier(_) => Field::new(nom_caracteristique, DataType::Int32, true),
        Caracteristique::Numeric(_) => Field::new(nom_caracteristique, DataType::Float64, true),
        Caracteristique::Textuel(_) => Field::new(nom_caracteristique, DataType::Utf8, true),
    }
}

fn create_vec_values(
    nom_caracteristique: &str,
    menage_list: &[Menage],
) -> KalkotoResult<Vec<Caracteristique>> {
    menage_list
        .iter()
        .map(|menage| menage.caracteristiques.get(nom_caracteristique))
        .map(|caracteristique| {
            caracteristique.ok_or_else(|| {
                From::from(OutputAdapterError::Custom(
                    "La caractéristique n'est pas présente".into(),
                ))
            })
        })
        .map(|caracteristique| match caracteristique {
            Ok(caracteristique) => Ok(caracteristique.clone()),
            Err(e) => Err(e),
        })
        .collect()
}

fn validate_menage_list(menage_list: &[Menage]) -> KalkotoResult<bool> {
    let is_valid = menage_list
        .iter()
        .tuple_windows::<(&Menage, &Menage)>()
        .map(|(menage_a, menage_b)| menage_a.compare_type_carac(menage_b))
        .all(|(is_faulty, _, _)| is_faulty);

    match is_valid {
        true => Ok(true),
        false => Err(From::from(OutputAdapterError::Custom(
            "Ménages aux caractéristiques incompatibles".into(),
        ))),
    }
}

fn extract_schema_from_menage(menage_blueprint: &Menage) -> KalkotoResult<Schema> {
    let mut caracteristiques_names: Vec<&String> =
        menage_blueprint.caracteristiques.keys().collect();
    caracteristiques_names.sort_unstable();

    //Extraction du schéma
    let fields: KalkotoResult<Vec<Field>> = caracteristiques_names
        .into_iter()
        .map(|name| (name, menage_blueprint.caracteristiques.get(name)))
        .map(|(name, opt_caracteristique)| {
            (
                name,
                opt_caracteristique.ok_or_else(|| {
                    KalkotoError::from(OutputAdapterError::Custom(format!(
                        "Caractéristique {} manquante",
                        name
                    )))
                }),
            )
        })
        .map(|(name, result_caract)| match (name, result_caract) {
            (name, Ok(caracteristique)) => Ok(create_field_from_caracteristique(
                name.as_ref(),
                caracteristique,
            )),
            _ => Err(From::from(OutputAdapterError::Custom(format!(
                "La caractéristique {} a un type non valide",
                name
            )))),
        })
        .collect();

    let schema = Schema::new(fields?);

    Ok(schema)
}

fn extract_values_from_menage_list(
    menage_blueprint: &Menage,
    menage_list: &[Menage],
) -> KalkotoResult<Vec<Arc<dyn Array>>> {
    let mut caracteristiques_names: Vec<&String> =
        menage_blueprint.caracteristiques.keys().collect();
    caracteristiques_names.sort_unstable();

    let values: KalkotoResult<Vec<Vec<Caracteristique>>> = caracteristiques_names
        .iter()
        .map(|name| create_vec_values(name, menage_list))
        .collect();
    let values = values?;

    values
        .clone()
        .into_iter()
        .map(|colonne| {
            let mut binding = colonne.iter().peekable();
            let caracteristique = binding.peek().ok_or_else(|| {
                KalkotoError::from(OutputAdapterError::Custom("Première colonne vide !".into()))
            });
            match caracteristique {
                Ok(Caracteristique::Entier(_)) => {
                    let values: Vec<i32> = colonne
                        .into_iter()
                        .map(|c| match c {
                            Caracteristique::Entier(i) => i,
                            _ => unreachable!(),
                        })
                        .collect();
                    Ok(Arc::new(Int32Array::from(values)) as Arc<dyn Array>)
                }
                Ok(Caracteristique::Numeric(_)) => {
                    let values: Vec<f64> = colonne
                        .into_iter()
                        .map(|c| match c {
                            Caracteristique::Numeric(i) => i,
                            _ => unreachable!(),
                        })
                        .collect();
                    Ok(Arc::new(Float64Array::from(values)) as Arc<dyn Array>)
                }
                Ok(Caracteristique::Textuel(_)) => {
                    let values: Vec<String> = colonne
                        .into_iter()
                        .map(|c| match c {
                            Caracteristique::Textuel(i) => i.clone(),
                            _ => unreachable!(),
                        })
                        .collect();
                    Ok(Arc::new(StringArray::from(values)) as Arc<dyn Array>)
                }
                _ => Err(From::from(OutputAdapterError::Custom("Les types des caractéristiques des ménages ne sont pas pris en charge (entier, double ou string)".into()))),
            }
        })
        .collect()
}

fn create_record_batch_from_menage_list(menage_list: &[Menage]) -> KalkotoResult<RecordBatch> {
    match validate_menage_list(menage_list)? {
        true => {
            let mut peekable = menage_list.iter().peekable();
            let first_valid_menage = peekable.peek().ok_or_else(|| {
                KalkotoError::from(OutputAdapterError::Custom(
                    "Aucun ménage dans le fichier d'input".into(),
                ))
            })?;

            //Extraction du schema
            let schema = extract_schema_from_menage(first_valid_menage)?;

            let arrays_data = extract_values_from_menage_list(first_valid_menage, menage_list)?;

            RecordBatch::try_new(Arc::new(schema), arrays_data)
                .map_err(|e| KalkotoError::from(OutputAdapterError::Arrow(e)))
        }
        false => Err(From::from(OutputAdapterError::Custom(
            "Impossible de créer le schéma".into(),
        ))),
    }
}

fn extract_schema_from_results_dict<V: AllowedValue>(
    valid_dict: &HashMap<String, V>,
) -> KalkotoResult<Schema> {
    let colonnes_names = valid_dict.keys();
    let mut colonnes_names: Vec<&String> = colonnes_names.collect::<Vec<&String>>();
    colonnes_names.sort_unstable();
    let fields: Vec<Field> = colonnes_names
        .iter()
        .map(|key| Field::new(*key, DataType::Float64, true))
        .collect();

    Ok(Schema::new(fields))
}

fn extract_values_from_list_results_dict<V: Default + AllowedValue + Copy + 'static>(
    list_dict_results: &[HashMap<String, V>],
) -> KalkotoResult<Vec<Float64Array>> {
    let mut binding = list_dict_results.iter().peekable();
    let colonnes_names = binding
        .peek()
        .ok_or_else(|| {
            KalkotoError::from(OutputAdapterError::Custom(
                "Dictionnaire de résultats vide".into(),
            ))
        })?
        .keys();

    let mut colonnes_names: Vec<&String> = colonnes_names.collect::<Vec<&String>>();
    colonnes_names.sort_unstable();
    let ncol = &colonnes_names.len();
    let nrow = list_dict_results.len();

    let mut fields_vec = vec![Float64Array::new_null(nrow); *ncol];

    for (index_colonne, nom_colonne) in colonnes_names.into_iter().enumerate() {
        let mut values_vec: Vec<V> = vec![V::default(); nrow];
        let _boucle: KalkotoResult<Vec<()>> = (0..nrow)
            .map(|index_row| -> KalkotoResult<()> {
                values_vec[index_row] = *list_dict_results
                    .get(index_row)
                    .ok_or_else(|| {
                        OutputAdapterError::Custom(
                            "Erreur dans le pivot des résultats pour présentation tabulaire".into(),
                        )
                    })?
                    .get(nom_colonne)
                    .ok_or_else(|| {
                        OutputAdapterError::Custom(format!("Colonne {} non présente", nom_colonne))
                    })?;

                Ok(())
            })
            .collect();

        let opt_values_vec: Vec<Option<f64>> = values_vec
            .into_iter()
            .map(|value| {
                // Convertit V en Option<f64> selon le type
                let opt_value: Option<f64> = match value {
                    // Si V est f64, on le convertit en Some(f64)
                    v if std::any::TypeId::of::<V>() == std::any::TypeId::of::<f64>() => {
                        // Sécurité : on sait que V est f64 ici
                        let v = unsafe { std::mem::transmute::<&V, &f64>(&v) };
                        Some(*v)
                    }
                    // Si V est Option<f64>, on le clone
                    v if std::any::TypeId::of::<V>() == std::any::TypeId::of::<Option<f64>>() => {
                        // Sécurité : on sait que V est Option<f64> ici
                        let v = unsafe { std::mem::transmute::<&V, &Option<f64>>(&v) };
                        *v
                    }
                    _ => None, // ne devrait jamais arriver grâce au trait AllowedValue
                };
                opt_value
            })
            .collect();

        let array_colonne = Float64Array::from(opt_values_vec);

        if let Some(element) = fields_vec.get_mut(index_colonne) {
            *element = array_colonne;
        }
    }

    Ok(fields_vec)
}

fn create_record_batch_from_list_dict_results<V: Default + AllowedValue + Copy + 'static>(
    dict_results_list: &[HashMap<String, V>],
) -> KalkotoResult<RecordBatch> {
    let mut peekable = dict_results_list.iter().peekable();
    let first_valid_dict_results = peekable.peek().ok_or(OutputAdapterError::Custom(
        "Aucun ménage dans la liste".into(),
    ))?;

    //Extraction du schema
    let schema = extract_schema_from_results_dict(first_valid_dict_results)?;

    let arrays_data = extract_values_from_list_results_dict(dict_results_list)?;

    let arrays_data = arrays_data
        .into_iter()
        .map(|colonne_data| Arc::new(colonne_data) as Arc<dyn Array>)
        .collect();

    Ok(RecordBatch::try_new(Arc::new(schema), arrays_data).map_err(OutputAdapterError::Arrow)?)
}

pub fn create_final_record_batch(
    menage_record_batch: &RecordBatch,
    results_record_batch: &RecordBatch,
) -> KalkotoResult<RecordBatch> {
    // Vérifier que les batches ont la même longueur
    match menage_record_batch.num_rows() == results_record_batch.num_rows() {
        false => Err(From::from(OutputAdapterError::Custom(
            "Le fichier ménages et la simulation n'ont pas la même longueur".into(),
        ))),
        true => {
            // Combiner les schemas
            let fields: Vec<Arc<Field>> = menage_record_batch
                .schema()
                .fields()
                .iter()
                .chain(results_record_batch.schema().fields().iter())
                .cloned()
                .collect();
            let merged_schema = Arc::new(Schema::new(fields));

            // Combiner les colonnes
            let mut columns = Vec::new();
            columns.extend_from_slice(menage_record_batch.columns());
            columns.extend_from_slice(results_record_batch.columns());

            // Créer le nouveau RecordBatch
            Ok(RecordBatch::try_new(merged_schema, columns).map_err(OutputAdapterError::Arrow)?)
        }
    }
}

pub fn write_final_record<P>(final_record: &RecordBatch, output_path: P) -> KalkotoResult<()>
where
    P: AsRef<Path>,
{
    let mut file = File::create(output_path.as_ref()).map_err(OutputAdapterError::IO)?;

    // create a new writer, the schema must be known in advance
    let mut writer = FileWriter::try_new(&mut file, &final_record.schema())
        .map_err(OutputAdapterError::Arrow)?;
    // write each batch to the underlying writer
    writer
        .write(final_record)
        .map_err(OutputAdapterError::Arrow)?;
    // When all batches are written, call finish to flush all buffers
    writer.finish().map_err(OutputAdapterError::Arrow)?;
    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_menage_list_valid_for_valide_liste() -> KalkotoResult<()> {
        let mut first_menage = Menage::new(1);
        first_menage
            .caracteristiques
            .insert(String::from("Age"), Caracteristique::Entier(30));

        first_menage
            .caracteristiques
            .insert(String::from("Revenu"), Caracteristique::Numeric(500.65f64));

        let mut second_menage = Menage::new(2);
        second_menage
            .caracteristiques
            .insert(String::from("Age"), Caracteristique::Entier(30));

        second_menage
            .caracteristiques
            .insert(String::from("Revenu"), Caracteristique::Numeric(400.45f64));

        let valid_vec_menage = vec![first_menage, second_menage];

        let wanted = true;
        let result = validate_menage_list(&valid_vec_menage)?;
        assert_eq!(wanted, result);

        Ok(())
    }

    #[test]
    fn test_menage_list_valid_for_unvalide_liste() -> KalkotoResult<()> {
        let mut first_menage = Menage::new(1);
        first_menage
            .caracteristiques
            .insert(String::from("Age"), Caracteristique::Entier(30));

        first_menage
            .caracteristiques
            .insert(String::from("Revenu"), Caracteristique::Numeric(500.65f64));

        let mut faulty_menage = Menage::new(2);
        faulty_menage
            .caracteristiques
            .insert(String::from("Age"), Caracteristique::Entier(30));

        faulty_menage
            .caracteristiques
            .insert(String::from("Salaire"), Caracteristique::Numeric(400.45f64));

        let valid_vec_menage = vec![first_menage, faulty_menage];

        let wanted = true;
        let result = validate_menage_list(&valid_vec_menage).is_err();
        assert_eq!(wanted, result);

        Ok(())
    }

    #[test]
    fn test_create_schema_invalid_for_invalid_liste() -> KalkotoResult<()> {
        let mut first_menage = Menage::new(1);
        first_menage
            .caracteristiques
            .insert(String::from("Age"), Caracteristique::Entier(30));

        first_menage
            .caracteristiques
            .insert(String::from("Revenu"), Caracteristique::Numeric(500.65f64));

        let mut second_menage = Menage::new(2);
        second_menage
            .caracteristiques
            .insert(String::from("Age"), Caracteristique::Entier(30));

        second_menage.caracteristiques.insert(
            String::from("Logement"),
            Caracteristique::Numeric(400.45f64),
        );

        let valid_vec_menage = vec![first_menage, second_menage];

        let record_batch_result = create_record_batch_from_menage_list(&valid_vec_menage);

        let wanted = true;
        let result = record_batch_result.is_err();
        assert_eq!(wanted, result);
        Ok(())
    }

    #[test]
    fn test_extract_schema_valid_dict_results() -> KalkotoResult<()> {
        let mut valid_dict = HashMap::new();
        valid_dict.insert(String::from("salaire_net"), 123f64);
        valid_dict.insert(String::from("rsa"), 0.005f64);

        let field_0 = Field::new("salaire_net", DataType::Float64, true);
        let field_1 = Field::new("rsa", DataType::Float64, true);

        let wanted = Schema::new(vec![field_1, field_0]); //le schema doit etre dans l'ordre alphabetique
        let result = extract_schema_from_results_dict(&valid_dict)?;

        assert_eq!(wanted, result);
        Ok(())
    }

    #[test]
    fn test_extract_values_valid_list_dict_results() -> KalkotoResult<()> {
        let mut valid_dict_0 = HashMap::new();
        valid_dict_0.insert(String::from("salaire_net"), 123f64);
        valid_dict_0.insert(String::from("rsa"), 0.005f64);

        let mut valid_dict_1 = HashMap::new();
        valid_dict_1.insert(String::from("salaire_net"), 234f64);
        valid_dict_1.insert(String::from("rsa"), 0.006f64);

        let valid_list_dict_results = vec![valid_dict_0, valid_dict_1];

        let array_salaire_net = Float64Array::from(vec![123f64, 234f64]);
        let array_rsa = Float64Array::from(vec![0.005f64, 0.006f64]);

        let wanted = vec![array_rsa, array_salaire_net];
        let result = extract_values_from_list_results_dict(&valid_list_dict_results)?;
        assert_eq!(wanted, result);
        Ok(())
    }

    #[test]
    fn test_extract_values_valid_list_dict_results_with_options() -> KalkotoResult<()> {
        let mut valid_dict_0 = HashMap::new();
        valid_dict_0.insert(String::from("salaire_net"), Some(123f64));
        valid_dict_0.insert(String::from("rsa"), None);

        let mut valid_dict_1 = HashMap::new();
        valid_dict_1.insert(String::from("salaire_net"), Some(234f64));
        valid_dict_1.insert(String::from("rsa"), Some(0.006f64));

        let valid_list_dict_results = vec![valid_dict_0, valid_dict_1];

        let array_salaire_net = Float64Array::from(vec![Some(123f64), Some(234f64)]);
        let array_rsa = Float64Array::from(vec![None, Some(0.006f64)]);

        let wanted = vec![array_rsa, array_salaire_net];
        let result = extract_values_from_list_results_dict(&valid_list_dict_results)?;

        assert_eq!(wanted, result);
        Ok(())
    }
}
