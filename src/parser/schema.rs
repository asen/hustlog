use crate::ParsedValueType;
use std::rc::Rc;

#[derive(Clone, Debug)]
pub struct ParserColDef {
    name: Rc<str>,
    pv_type: ParsedValueType,
}

impl ParserColDef {
    pub fn new(name: &str, pv_type: &ParsedValueType) -> Self {
        Self {
            name: Rc::from(name),
            pv_type: pv_type.clone(),
        }
    }

    pub fn name(&self) -> &Rc<str> {
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
