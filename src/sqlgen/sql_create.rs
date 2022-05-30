use crate::parser::{ParsedValueType, ParserColDef, ParserSchema};
use std::sync::Arc;
use crate::ql_processor::QlSchema;

pub struct SqlCreateCol {
    name: Arc<str>,
    sql_type: Arc<str>,
    extra_spec: Arc<str>, // NOT NULL DEFAULT ...
}

impl SqlCreateCol {
    #[cfg(test)]
    pub fn new(
        name: Arc<str>,
        sql_type: Arc<str>,
        extra_spec: Arc<str>, // NOT NULL DEFAULT ...
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
            sql_type: Arc::from(sql_type),
            extra_spec: Arc::from(""),
        }
    }

    fn create_def(&self) -> String {
        [&self.name, " ", &self.sql_type, " ", &self.extra_spec].join("")
    }
}

pub struct SqlCreateSchema {
    table_name: Arc<str>,
    col_defs: Vec<SqlCreateCol>,
    pre_name_opts: Arc<str>, // "if not exists"
    table_opts: Arc<str>,    // stuff to add at the end of the statement
}

impl SqlCreateSchema {
    #[cfg(test)]
    pub fn new(
        table_name: Arc<str>,
        col_defs: Vec<SqlCreateCol>,
        pre_name_opts: Arc<str>, // "if not exists"
        table_opts: Arc<str>,    // stuff to add at the end of the statement
    ) -> Self {
        Self {
            table_name,
            col_defs,
            pre_name_opts,
            table_opts,
        }
    }

    pub fn from_ql_schema(schema: &QlSchema) -> Self {
        let col_defs = schema
            .col_defs()
            .iter()
            .map(|&x| SqlCreateCol::from_parser_col_def(x))
            .collect();
        Self {
            table_name: Arc::from(schema.name()),
            col_defs,
            pre_name_opts: Arc::from(""),
            table_opts: Arc::from(""),
        }
    }

    // pub fn from_grok_schema(schema: &GrokSchema) -> Self {
    //     let col_defs = schema
    //         .col_defs()
    //         .iter()
    //         .map(|&x| SqlCreateCol::from_parser_col_def(x))
    //         .collect();
    //     Self {
    //         table_name: Rc::from(schema.name()),
    //         col_defs,
    //         pre_name_opts: Rc::from(""),
    //         table_opts: Rc::from(""),
    //     }
    // }

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
    use std::sync::Arc;

    #[test]
    fn test_sql_create_schema() {
        let cols = vec![
            SqlCreateCol::new(
                Arc::from("timestamp"),
                Arc::from("TIMESTAMP"),
                Arc::from("NOT NULL"),
            ),
            SqlCreateCol::new(
                Arc::from("logsource"),
                Arc::from("VARCHAR(255)"),
                Arc::from(""),
            ),
            SqlCreateCol::new(
                Arc::from("progname"),
                Arc::from("VARCHAR(40)"),
                Arc::from(""),
            ),
            SqlCreateCol::new(Arc::from("pid"), Arc::from("INT"), Arc::from("")),
            SqlCreateCol::new(Arc::from("message"), Arc::from("TEXT"), Arc::from("")),
        ];
        let sc = SqlCreateSchema::new(Arc::from("syslog"), cols, Arc::from(""), Arc::from(""));

        println!("RESULT:\n{}", sc.get_create_sql())
    }
}
