use std::error::Error;
use std::fmt;
use sqlparser::ast::{Query, Statement, SetExpr, Select, Expr};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};

#[derive(Debug, Clone, PartialEq)]
pub enum SqlParserError {
    ParserError(String),
    QueryNotSupportedError(String),
}

impl SqlParserError {
    pub fn not_supported(s: &str) -> SqlParserError {
        SqlParserError::QueryNotSupportedError(s.to_string())
    }

    pub fn parse_error(s: &str) -> SqlParserError {
        SqlParserError::ParserError(s.to_string())
    }
}

impl fmt::Display for SqlParserError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Log parse error: {:?}", self)
    }
}

impl Error for SqlParserError {}


#[derive(Debug)]
pub struct SqlSelectQuery {
    ast_query: Box<Query>,
}

impl SqlSelectQuery {
    pub fn get_select(&self) -> &Select {
        if let SetExpr::Select(s) = &self.ast_query.body {
            s
        } else {
            panic!("BUG: Only SELECT queries should be allowed, please fix the parsing/validation logic");
        }
    }

    pub fn get_limit(&self) -> Option<&Expr> {
        self.ast_query.limit.as_ref()
    }

    pub fn get_offset(&self) -> Option<&Expr> {
        if self.ast_query.offset.is_some() {
            Some(&self.ast_query.offset.as_ref().unwrap().value)
        } else {
            None
        }
    }

    // pub fn get_order_by(&self) -> &Vec<OrderByExpr> {
    //     &self.ast_query.order_by
    // }
}

impl SqlSelectQuery {

    fn from_query(q: &Query) -> Result<SqlSelectQuery, SqlParserError> {
        if q.with.is_some() {
            return Err(SqlParserError::not_supported("WITH is not supported"))
        }
        if q.lock.is_some() {
            return Err(SqlParserError::not_supported("LOCK is not supported"))
        }
        if q.fetch.is_some() {
            return Err(SqlParserError::not_supported("FETCH is currently not supported"))
        }
        if let SetExpr::Select(_) = &q.body {
            // q.body;
            // q.limit;
            // q.offset;
            // q.order_by;
            Ok(SqlSelectQuery {
                ast_query: Box::new(q.clone()),
            })
        } else {
            Err(SqlParserError::not_supported(
                "Only SELECT queries are supported for now"
            ))
        }
    }

    pub fn new(sql: &str) -> Result<SqlSelectQuery, SqlParserError> {
        let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...
        let ast: Result<Vec<Statement>, ParserError> = Parser::parse_sql(&dialect, sql);
        match ast {
            Ok(vec) => {
                if vec.len() > 1 {
                    Err(SqlParserError::not_supported(
                        "Can not process multiple statements for now"
                    ))
                } else {
                    let stmt = vec.iter().next().unwrap();
                    if let Statement::Query(b) = stmt {
                        SqlSelectQuery::from_query(b)
                    } else {
                        Err(SqlParserError::not_supported(
                            "Only queries are supported for now"
                        ))
                    }
                }
            }
            Err(x) => Err(SqlParserError::parse_error(&x.to_string())),
        }
    }

}

#[cfg(test)]
fn parse_sql(sql: &str) -> Result<SqlSelectQuery, SqlParserError> {
    SqlSelectQuery::new(sql)
}

#[test]
fn test_parse_sql() {
    let sql = "SELECT coldef(a, ts, \"ala bala\"), b, 123, myfunc(b) \
           FROM table_1 \
           WHERE a > b AND b < 100 \
           ORDER BY a DESC, b";

    let ast = parse_sql(sql).unwrap();
    println!("AST: {:?}", ast);
}

#[test]
fn test_parse_sql2() {
    let sql = "SELECT coldef(a,int), coldef(b,int) \
           FROM table_1(\"arg 1\") \
           WHERE  a > b AND b < 100 \
           ORDER BY a DESC, b";

    let ast = parse_sql(sql).unwrap();
    let s = ast.get_select();
    println!("BODY: {:?}", s.from);
}

// AST: SqlSelectQuery { ast_query:
// Query {
//      with: None,
//      body: Select(
//          Select {
//              distinct: false,
//              top: None,
//              projection: [
//                  UnnamedExpr(Function(Function { name: ObjectName([Ident { value: "coldef", quote_style: None }]),
//                      args: [Unnamed(Expr(Identifier(Ident { value: "a", quote_style: None }))),
//                          Unnamed(Expr(Identifier(Ident { value: "ts", quote_style: None }))),
//                          Unnamed(Expr(Identifier(Ident { value: "ala bala", quote_style: Some('"') })))],
//                      over: None, distinct: false
//                      })),
//                  UnnamedExpr(Identifier(Ident { value: "b", quote_style: None })),
//                  UnnamedExpr(Value(Number("123", false))),
//                  UnnamedExpr(Function(Function { name: ObjectName([Ident { value: "myfunc", quote_style: None }]),
//                      args: [Unnamed(Expr(Identifier(Ident { value: "b", quote_style: None })))],
//                      over: None, distinct: false
//                      }))
//                  ],
//              into: None,
//              from: [
//                  TableWithJoins {
//                      relation: Table { name: ObjectName([Ident { value: "table_1", quote_style: None }]),
//                                        alias: None, args: [], with_hints: [] },
//                      joins: [] }
//              ],
//              lateral_views: [],
//              selection: Some(BinaryOp {
//                  left: BinaryOp {
//                      left: Identifier(Ident { value: "a", quote_style: None }),
//                      op: Gt,
//                      right: Identifier(Ident { value: "b", quote_style: None })
//                  },
//                  op: And,
//                  right: BinaryOp {
//                      left: Identifier(Ident { value: "b", quote_style: None }),
//                      op: Lt,
//                      right: Value(Number("100", false))
//                  }
//              }),
//              group_by: [],
//              cluster_by: [],
//              distribute_by: [],
//              sort_by: [],
//              having: None
//          }),  // Select
//      order_by: [
//          OrderByExpr { expr: Identifier(Ident { value: "a", quote_style: None }), asc: Some(false), nulls_first: None },
//          OrderByExpr { expr: Identifier(Ident { value: "b", quote_style: None }), asc: None, nulls_first: None }
//      ],
//      limit: None,
//      offset: None,
//      fetch: None,
//      lock: None } }
