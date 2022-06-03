use crate::parser::ParsedValueType;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct ParserColDef {
    name: Arc<str>,
    pv_type: ParsedValueType,
}

impl ParserColDef {
    pub fn new(name: &str, pv_type: &ParsedValueType) -> Self {
        Self {
            name: Arc::from(name),
            pv_type: pv_type.clone(),
        }
    }

    pub fn name(&self) -> &Arc<str> {
        &self.name
    }

    pub fn pv_type(&self) -> &ParsedValueType {
        &self.pv_type
    }
}

pub trait ParserSchema {
    fn name(&self) -> &str;
    fn col_defs(&self) -> Vec<&ParserColDef>;
}

pub type DynParserSchema = Arc<dyn ParserSchema + Send + Sync>;
