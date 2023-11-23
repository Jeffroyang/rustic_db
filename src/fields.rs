use crate::types::{Type, STRING_SIZE};

// Wrapper for different types of fields
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum FieldVal {
    IntField(IntField),
    StringField(StringField),
}

impl FieldVal {
    // Extracts the inner IntField
    fn into_int(self) -> Option<IntField> {
        match self {
            FieldVal::IntField(int_field) => Some(int_field),
            _ => None,
        }
    }

    // Extracts the inner StringField
    fn into_string(self) -> Option<StringField> {
        match self {
            FieldVal::StringField(string_field) => Some(string_field),
            _ => None,
        }
    }
}

// Trait for different types of fields
pub trait Field {
    // Get the type of the field
    fn get_type(&self) -> Type;
    // Serialize the field into bytes
    fn serialize(&self) -> Vec<u8>;
}

#[derive(Debug, PartialEq, Eq, Clone)]
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

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct StringField {
    value: String,
    len: u32,
}

impl StringField {
    pub fn new(value: String, len: u32) -> Self {
        StringField { value, len }
    }
}

impl Field for StringField {
    fn get_type(&self) -> Type {
        Type::StringType
    }

    fn serialize(&self) -> Vec<u8> {
        let mut bytes = vec![0; STRING_SIZE + 4];
        bytes[0..4].copy_from_slice(&self.len.to_be_bytes());
        // copy as many bytes as possible from string and pad with 0s
        let str_bytes = self.value.as_bytes();
        let copy_len = std::cmp::min(str_bytes.len(), STRING_SIZE);
        bytes[4..4 + copy_len].copy_from_slice(&str_bytes[..copy_len]);
        bytes
    }
}
