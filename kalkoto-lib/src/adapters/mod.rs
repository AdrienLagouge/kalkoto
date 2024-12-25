use crate::entities::menage::Menage;

mod csv_input_adapter;
mod toml_input_adapter;

#[derive(thiserror::Error, Debug)]
pub enum MenageListAdapterError {
    #[error(transparent)]
    IO(#[from] std::io::Error),

    #[error("Erreur à la validation de la liste des cas-types pour le ménage {0}")]
    ValidationError(String),
}

//#[derive(thiserror::Error, Debug)]
//pub enum PolicyAdapterError {}
#[derive(Debug, Default)]
pub struct MenageInputValidator<'a> {
    pub input_buf: &'a [u8],
    pub liste_menage: Option<Vec<Menage>>,
}

pub trait MenageListAdapter {
    fn validate_byte_buffer(&self, input_buf: &[u8])
        -> Result<Vec<Menage>, MenageListAdapterError>;

    fn create_from_byte_buffer<'a>(
        &'a self,
        input_buf: &'a [u8],
    ) -> Result<MenageInputValidator<'a>, MenageListAdapterError> {
        let liste_menages = self.validate_byte_buffer(input_buf)?;
        let value = MenageInputValidator {
            input_buf,
            liste_menage: Some(liste_menages),
        };
        Ok(value)
    }
}
