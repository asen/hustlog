use crate::{ParsedMessage, ParsedValue, RawMessage};
use std::rc::Rc;

pub struct ResultRow {
    msg: ParsedMessage,
    computed: Vec<(Rc<str>, ParsedValue)>,
}

impl ResultRow {
    pub fn get_raw(&self) -> &RawMessage {
        self.msg.get_raw()
    }
    // pub fn get_parsed(&self) -> &ParsedData {
    //     self.msg.get_parsed()
    // }
    pub fn get_computed(&self) -> &Vec<(Rc<str>, ParsedValue)> {
        &self.computed
    }
}

pub struct ResultTable {
    rows: Vec<ResultRow>,
}

impl ResultTable {
    pub fn new() -> ResultTable {
        ResultTable { rows: Vec::new() }
    }

    // pub fn add_cols(&mut self, cols: Vec<ParsedValue>) -> () {
    //     self.add_row(ResultRow::new(cols));
    // }

    pub fn add_row(&mut self, msg: ParsedMessage, computed: Vec<(Rc<str>, ParsedValue)>) -> () {
        self.rows.push(ResultRow { msg, computed });
    }

    pub fn get_rows(&self) -> &Vec<ResultRow> {
        &self.rows
    }

    // pub fn sort(&mut self, by: &Expr ) {
    //     self.rows.sort_by(|x,y| {
    //
    //     })
    // }
}
