use crate::fields::{FieldVal, IntField, StringField};

pub const STRING_SIZE: usize = 256;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Type {
    // Define FieldType variants
    IntType,
    StringType,
}

impl Type {
    pub fn get_len(&self) -> usize {
        match self {
            Type::IntType => 4,
            Type::StringType => STRING_SIZE + 4,
        }
    }

    pub fn parse(&self, bytes: &[u8]) -> Result<FieldVal, String> {
        match self {
            Type::IntType => {
                let mut int_bytes = [0; 4];
                int_bytes.copy_from_slice(&bytes[..4]);
                Ok(FieldVal::IntField(IntField::new(i32::from_be_bytes(
                    int_bytes,
                ))))
            }
            Type::StringType => {
                let len = bytes[0] as usize;
                let mut string_bytes = [0; STRING_SIZE];
                string_bytes.copy_from_slice(&bytes[1..=len]);
                Ok(FieldVal::StringField(StringField::new(
                    String::from_utf8(string_bytes.to_vec()).unwrap(),
                    len,
                )))
            }
        }
    }
}
