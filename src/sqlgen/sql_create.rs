use crate::{GrokSchema, ParsedValueType, ParserColDef, ParserSchema};
use std::rc::Rc;

pub struct SqlCreateCol {
    name: Rc<str>,
    sql_type: Rc<str>,
    extra_spec: Rc<str>, // NOT NULL DEFAULT ...
}

impl SqlCreateCol {

    #[cfg(test)]
    pub fn new(
        name: Rc<str>,
        sql_type: Rc<str>,
        extra_spec: Rc<str>, // NOT NULL DEFAULT ...
    ) -> Self {
        Self {
            name,
            sql_type,
            extra_spec,
        }
    }

    fn from_parser_col_def(pcd: &ParserColDef) -> SqlCreateCol {
        let sql_type = match pcd.pv_type() {
            ParsedValueType::NullType => {
                "NULL" // this shouldn't happen?
            }
            ParsedValueType::BoolType => "BOOLEAN",
            ParsedValueType::LongType => "BIGINT",
            ParsedValueType::DoubleType => "DOUBLE",
            ParsedValueType::TimeType(_) => "TIMESTAMP",
            ParsedValueType::StrType => "VARCHAR",
        };
        Self {
            name: pcd.name().clone(),
            sql_type: Rc::from(sql_type),
            extra_spec: Rc::from(""),
        }
    }

    fn create_def(&self) -> String {
        [&self.name, " ", &self.sql_type, " ", &self.extra_spec].join("")
    }
}

pub struct SqlCreateSchema {
    table_name: Rc<str>,
    col_defs: Vec<SqlCreateCol>,
    pre_name_opts: Rc<str>, // "if not exists"
    table_opts: Rc<str>,    // stuff to add at the end of the statement
}

impl SqlCreateSchema {

    #[cfg(test)]
    pub fn new(
        table_name: Rc<str>,
        col_defs: Vec<SqlCreateCol>,
        pre_name_opts: Rc<str>, // "if not exists"
        table_opts: Rc<str>,    // stuff to add at the end of the statement
    ) -> Self {
        Self {
            table_name,
            col_defs,
            pre_name_opts,
            table_opts,
        }
    }

    pub fn from_grok_schema(schema: &GrokSchema) -> Self {
        let col_defs = schema
            .col_defs()
            .iter()
            .map(|&x| SqlCreateCol::from_parser_col_def(x))
            .collect();
        Self {
            table_name: Rc::from(schema.name()),
            col_defs,
            pre_name_opts: Rc::from(""),
            table_opts: Rc::from(""),
        }
    }

    pub fn get_create_sql(&self) -> String {
        let col_defs_str = self
            .col_defs
            .iter()
            .map(|cd| cd.create_def())
            .collect::<Vec<_>>()
            .join(",\n");

        [
            "CREATE TABLE ",
            &self.table_name,
            " ",
            &self.pre_name_opts,
            "(\n",
            &col_defs_str,
            ") ",
            &self.table_opts,
            ";\n",
        ]
        .join("")
    }
}

#[cfg(test)]
mod test {
    use crate::sqlgen::sql_create::{SqlCreateCol, SqlCreateSchema};
    use std::rc::Rc;

    #[test]
    fn test_sql_create_schema() {
        let cols = vec![
            SqlCreateCol::new(
                Rc::from("timestamp"),
                Rc::from("TIMESTAMP"),
                Rc::from("NOT NULL"),
            ),
            SqlCreateCol::new(
                Rc::from("logsource"),
                Rc::from("VARCHAR(255)"),
                Rc::from(""),
            ),
            SqlCreateCol::new(Rc::from("progname"), Rc::from("VARCHAR(40)"), Rc::from("")),
            SqlCreateCol::new(Rc::from("pid"), Rc::from("INT"), Rc::from("")),
            SqlCreateCol::new(Rc::from("message"), Rc::from("TEXT"), Rc::from("")),
        ];
        let sc = SqlCreateSchema::new(Rc::from("syslog"), cols, Rc::from(""), Rc::from(""));

        println!("RESULT:\n{}", sc.get_create_sql())
    }
}
