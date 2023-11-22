use crate::types::{Type, STRING_SIZE};

#[derive(Debug, PartialEq, Clone)]
pub enum FieldVal {
    IntField(IntField),
    StringField(StringField),
}

impl FieldVal {
    fn into_int(self) -> Option<IntField> {
        match self {
            FieldVal::IntField(int_field) => Some(int_field),
            _ => None,
        }
    }

    fn into_string(self) -> Option<StringField> {
        match self {
            FieldVal::StringField(string_field) => Some(string_field),
            _ => None,
        }
    }
}

pub trait Field {
    fn get_type(&self) -> Type;
    fn serialize(&self) -> Vec<u8>;
}

#[derive(Debug, PartialEq, Clone)]
pub struct IntField {
    value: i32,
}

impl IntField {
    pub fn new(value: i32) -> Self {
        IntField { value }
    }
}

impl Field for IntField {
    fn get_type(&self) -> Type {
        Type::IntType
    }

    fn serialize(&self) -> Vec<u8> {
        self.value.to_be_bytes().to_vec()
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct StringField {
    value: String,
    len: usize,
}

impl StringField {
    pub fn new(value: String, len: usize) -> Self {
        StringField { value, len }
    }
}

impl Field for StringField {
    fn get_type(&self) -> Type {
        Type::StringType
    }

    fn serialize(&self) -> Vec<u8> {
        let mut bytes = vec![0; STRING_SIZE + 1];
        bytes[0] = self.len as u8;
        bytes[1..=self.len].copy_from_slice(self.value[..self.len].as_bytes());
        bytes
    }
}
