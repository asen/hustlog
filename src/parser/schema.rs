use crate::parser::ParsedValueType;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct ParserColDef {
    name: Arc<str>,
    pv_type: ParsedValueType,
    required: bool,
}

impl ParserColDef {
    pub fn new(name: &str, pv_type: &ParsedValueType, required: bool) -> Self {
        Self {
            name: Arc::from(name),
            pv_type: pv_type.clone(),
            required,
        }
    }

    pub fn name(&self) -> &Arc<str> {
        &self.name
    }

    pub fn pv_type(&self) -> &ParsedValueType {
        &self.pv_type
    }

    pub fn required(&self) -> bool {
        self.required
    }
}

pub trait ParserSchema {
    fn output_name(&self) -> &str;
    fn col_defs(&self) -> Vec<&ParserColDef>;
}

pub type DynParserSchema = Arc<dyn ParserSchema + Send + Sync>;
