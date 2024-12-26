use crate::adapters::MenageListAdapterError;
use crate::entities::menage::Menage;
use csv::{Reader, ReaderBuilder};
use std::{
    fs::{write, File},
    io::{self, Read},
    path::Path,
};

#[derive(Debug)]
pub struct CsvInputAdapter<R: io::Read> {
    pub reader: Option<Reader<R>>,
}

impl<R: io::Read> CsvInputAdapter<R> {
    pub fn new() -> Self {
        CsvInputAdapter::<R> { reader: None }
    }
}

impl CsvInputAdapter<&[u8]> {
    pub fn from_buf(input_buf: &'static [u8]) -> Self {
        let rdr = ReaderBuilder::new().delimiter(b';').from_reader(input_buf);
        Self { reader: Some(rdr) }
    }

    pub fn from_path<P>(
        path: P,
        buf_string: &'static mut String,
    ) -> Result<Self, MenageListAdapterError>
    where
        P: AsRef<Path>,
    {
        let mut f = match File::open(path) {
            Ok(file) => file,
            Err(e) => return Err(MenageListAdapterError::IO(e)),
        };

        // read the whole file
        let _ = match f.read_to_string(buf_string) {
            Ok(nbytes) => nbytes,
            Err(e) => return Err(MenageListAdapterError::IO(e)),
        };

        let output_slice = buf_string.as_bytes();
        Ok(CsvInputAdapter::<&[u8]>::from_buf(output_slice))
    }
}

// impl MenageListAdapter for CsvInputAdapter<&[u8]> {
//     fn validate_byte_buffer(&self) -> Result<Vec<Menage>, MenageListAdapterError> {
//         let records = match &self.reader {
//             None => {
//                 return Err(MenageListAdapterError::IO(
//                     "Fichier non encore lu !".to_owned(),
//                 ))
//             }
//             Some(records) => records,
//         };
//
//         let mut vec_results = vec![];
//
//         Ok(vec_results)
//     }
// }
