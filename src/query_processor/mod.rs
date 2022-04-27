use crate::parser::*;
use crate::query::*;
use sqlparser::ast::{BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, ObjectName, SelectItem, UnaryOperator, Value};
use std::error::Error;
use std::fmt;
use std::ops::{Add, Div, Mul, Rem, Sub};
use std::rc::Rc;

#[derive(Debug, Clone)]
pub struct QueryError(String);

impl QueryError {
    pub fn new(s: &str) -> QueryError {
        QueryError(s.to_string())
    }

    pub fn not_supported(what: &str) -> QueryError {
        QueryError(format!("Feature not supported {}", what))
    }

    pub fn not_impl(what: &str) -> QueryError {
        QueryError(format!("Feature not implemented yet {}", what))
    }

    pub fn unexpected(what: &str) -> QueryError {
        QueryError(format!("Unexpected error: {}", what))
    }

    pub fn incompatible_types(ltype: &ParsedValue, rtype: &ParsedValue, op: &BinaryOperator) -> QueryError {
        QueryError(format!("Incompatible types for op: {:?} lval={:?} rval={:?}", op, ltype, rtype))
    }
}
impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Query error: {}", self.0)
    }
}

impl Error for QueryError {}

pub struct ResultTable {
    rows: Vec<ParsedMessage>,
}

impl ResultTable {
    pub fn new() -> ResultTable {
        ResultTable { rows: Vec::new() }
    }

    // pub fn add_cols(&mut self, cols: Vec<ParsedValue>) -> () {
    //     self.add_row(ResultRow::new(cols));
    // }

    pub fn add_row(&mut self, row: ParsedMessage) -> () {
        self.rows.push(row);
    }

    pub fn get_rows(&self) -> &Vec<ParsedMessage> {
        &self.rows
    }

    // pub fn sort(&mut self, by: &Expr ) {
    //     self.rows.sort_by(|x,y| {
    //
    //     })
    // }
}

fn eval_aritmethic_op<T>(lval: T, rval: T, op: &BinaryOperator) -> Result<T, QueryError>
    where T: Add<Output = T> +
    Mul<Output = T> +
    Sub<Output = T> +
    Div<Output = T> +
    Rem<Output = T>{
    match op {
        BinaryOperator::Plus => {
            Ok(lval + rval)
        }
        BinaryOperator::Minus => {
            Ok(lval - rval)
        }
        BinaryOperator::Multiply => {
            Ok(lval * rval)
        }
        BinaryOperator::Divide => {
            Ok(lval / rval)
        }
        BinaryOperator::Modulo => {
            Ok(lval % rval)
        }
        // BinaryOperator::BitwiseOr => {}
        // BinaryOperator::BitwiseAnd => {}
        // BinaryOperator::BitwiseXor => {}
        // BinaryOperator::PGBitwiseShiftLeft => {}
        // BinaryOperator::PGBitwiseShiftRight => {}
        _ => Err(QueryError::unexpected("Invalid arithmetic op"))
    }
}

fn object_name_to_string(onm: &ObjectName) -> String {
    onm.0.iter().map(|x| {
        x.value.as_str()
    }).collect::<Vec<&str>>().join(",")
}

fn func_arg_to_pv(arg: &FunctionArg, ctx: Option<&ParsedData>) -> Result<ParsedValue, QueryError> {
    match arg {
        FunctionArg::Named { .. } => {
            Err(QueryError::not_supported("Named function arguments are not supported yet"))
        }
        FunctionArg::Unnamed(fax) => {
            match fax {
                FunctionArgExpr::Expr(xp) => {
                    eval_expr(xp, ctx)
                }
                FunctionArgExpr::QualifiedWildcard(_) => {
                    Err(QueryError::not_supported("FunctionArgExpr::QualifiedWildcard"))
                }
                FunctionArgExpr::Wildcard => {
                    Ok(ParsedValue::StrVal(Rc::new("*".to_string())))
                }
            }
        }
    }
}

fn eval_function_date(fun: &Function, ctx: Option<&ParsedData>) -> Result<ParsedValue, QueryError> {
    let mut args_iter = fun.args.iter();
    let cur_arg: &FunctionArg = args_iter.next().ok_or(QueryError::new("DATE function requires arguments"))?;
    let curv = func_arg_to_pv(cur_arg, ctx)?;
    let tformat = if let ParsedValue::StrVal(rs) = curv {
        Ok(TimeTypeFormat::new(rs.as_str()))
    } else { Err(QueryError::new("DATE function first argument must be a time format string")) }?;
    let cur_arg: &FunctionArg = args_iter.next().ok_or(QueryError::new("DATE function requires arguments"))?;
    let curv = func_arg_to_pv(cur_arg, ctx)?;
    match curv {
        ParsedValue::TimeVal(d) => { Ok(ParsedValue::TimeVal(d)) }
        ParsedValue::StrVal(s) => {
            str2val(s.as_str(), &ParsedValueType::TimeType(tformat))
                .ok_or(QueryError::new(
                    &format!("Failed to parse date from string {}",
                             s.as_str())))
        }
        x => Err(QueryError::not_impl(
            &format!("Unsupported argument type for DATE function {:?}", x)))
    }

}

fn eval_function(fun: &Function, ctx: Option<&ParsedData>) -> Result<ParsedValue, QueryError> {
    let fun_name = object_name_to_string(&fun.name);
    match fun_name.as_str() {
        "DATE" => eval_function_date(fun, ctx),
        _ => Err(QueryError::not_supported(&format!("Function: {:?}", fun)))
    }
}

fn eval_expr(expr: &Expr, ctx: Option<&ParsedData>) -> Result<ParsedValue, QueryError> {
    match expr {
        Expr::Identifier(x) => {
            if x.quote_style.is_some() || ctx.is_none() {
                Ok(ParsedValue::StrVal(Rc::new(x.value.clone())))
            } else {
                if let Some(val) = ctx.unwrap().get_value(&x.value) {
                    Ok(val.clone())
                } else {
                    Ok(ParsedValue::NullVal)
                }
            }
        }
        Expr::CompoundIdentifier(_) => Err(QueryError::not_impl("Expr::CompoundIdentifier")),
        Expr::IsNull(x) => {
            let res = eval_expr(x, ctx)?;
            Ok(ParsedValue::BoolVal(res == ParsedValue::NullVal))
        }
        Expr::IsNotNull(x) => {
            let res = eval_expr(x, ctx)?;
            Ok(ParsedValue::BoolVal(res != ParsedValue::NullVal))
        }
        Expr::IsDistinctFrom(_, _) => Err(QueryError::not_impl("Expr::IsDistinctFrom")),
        Expr::IsNotDistinctFrom(_, _) => Err(QueryError::not_impl("Expr::IsNotDistinctFrom")),
        Expr::InList { .. } => Err(QueryError::not_impl("Expr::InList")),
        Expr::InSubquery { .. } => Err(QueryError::not_impl("Expr::InSubquery")),
        Expr::InUnnest { .. } => Err(QueryError::not_impl("Expr::InUnnest")),
        Expr::Between { .. } => Err(QueryError::not_impl("Expr::Between")),
        Expr::BinaryOp { left, op, right } => {
            let lval = eval_expr(left, ctx)?;
            let rval = || { // using a closure for lazy evaluation
                let rv = eval_expr(right, ctx)?;
                Ok(rv)
            };
            // another closure for lazy eval
            let arithmetic_op = |op: &BinaryOperator| {
                let rvalv: ParsedValue = rval()?;
                match (&lval, &rvalv) {
                    (ParsedValue::LongVal(lv), ParsedValue::LongVal(rv)) =>  {
                        Ok(ParsedValue::LongVal(eval_aritmethic_op(*lv, *rv, &op)?))
                    }
                    (ParsedValue::DoubleVal(lv), ParsedValue::LongVal(rv)) =>  {
                        let rvd = *rv as f64;
                        Ok(ParsedValue::DoubleVal(eval_aritmethic_op(*lv, rvd, &op)?))
                    }
                    (ParsedValue::LongVal(lv), ParsedValue::DoubleVal(rv)) =>  {
                        let lvd = *lv as f64;
                        Ok(ParsedValue::DoubleVal(eval_aritmethic_op(lvd, *rv, &op)?))
                    }
                    (ParsedValue::DoubleVal(lv), ParsedValue::DoubleVal(rv)) =>  {
                        Ok(ParsedValue::DoubleVal(eval_aritmethic_op(*lv, *rv, &op)?))
                    }
                    _ => Err(QueryError::incompatible_types(&lval, &rvalv, &op))
                }
            };
            match op {
                BinaryOperator::Plus => arithmetic_op(&op),
                BinaryOperator::Minus => arithmetic_op(&op),
                BinaryOperator::Multiply => arithmetic_op(&op),
                BinaryOperator::Divide => arithmetic_op(&op),
                BinaryOperator::Modulo => arithmetic_op(&op),
                BinaryOperator::StringConcat => {
                    let lstr = lval.to_rc_str();
                    let rstr = rval()?.to_rc_str();
                    Ok(ParsedValue::StrVal(Rc::new([lstr.as_str(), rstr.as_str()].join(""))))
                },
                BinaryOperator::Gt => Ok(ParsedValue::BoolVal(lval > rval()?)),
                BinaryOperator::Lt => Ok(ParsedValue::BoolVal(lval < rval()?)),
                BinaryOperator::GtEq => Ok(ParsedValue::BoolVal(lval >= rval()?)),
                BinaryOperator::LtEq => Ok(ParsedValue::BoolVal(lval > rval()?)),
                BinaryOperator::Spaceship => Err(QueryError::not_impl("BinaryOperator::Spaceship")),
                BinaryOperator::Eq => Ok(ParsedValue::BoolVal(lval == rval()?)),
                BinaryOperator::NotEq => Ok(ParsedValue::BoolVal(lval != rval()?)),
                BinaryOperator::And => Ok(ParsedValue::BoolVal(
                    lval.as_bool().unwrap_or(false) && rval()?.as_bool().unwrap_or(false),
                )),
                BinaryOperator::Or => Ok(ParsedValue::BoolVal(
                    lval.as_bool().unwrap_or(false) || rval()?.as_bool().unwrap_or(false),
                )),
                BinaryOperator::Xor => Ok(ParsedValue::BoolVal(
                    lval.as_bool().unwrap_or(false) != rval()?.as_bool().unwrap_or(false),
                )),
                BinaryOperator::Like => Err(QueryError::not_impl("BinaryOperator::Like")),
                BinaryOperator::NotLike => Err(QueryError::not_impl("BinaryOperator::NotLike")),
                BinaryOperator::ILike => Err(QueryError::not_impl("BinaryOperator::ILike")),
                BinaryOperator::NotILike => Err(QueryError::not_impl("BinaryOperator::NotILike")),

                BinaryOperator::BitwiseOr => Err(QueryError::not_impl("BinaryOperator::BitwiseOr")),
                BinaryOperator::BitwiseAnd => Err(QueryError::not_impl("BinaryOperator::BitwiseAnd")),
                BinaryOperator::BitwiseXor => Err(QueryError::not_impl("BinaryOperator::BitwiseXor")),
                BinaryOperator::PGBitwiseXor => Err(QueryError::not_impl("BinaryOperator::PGBitwiseXor")),
                BinaryOperator::PGBitwiseShiftLeft => Err(QueryError::not_impl("BinaryOperator::PGBitwiseShiftLeft")),
                BinaryOperator::PGBitwiseShiftRight => Err(QueryError::not_impl("BinaryOperator::PGBitwiseShiftRight")),

                BinaryOperator::PGRegexMatch => Err(QueryError::not_impl("BinaryOperator::PGRegexMatch")),
                BinaryOperator::PGRegexIMatch => Err(QueryError::not_impl("BinaryOperator::PGRegexIMatch")),
                BinaryOperator::PGRegexNotMatch => Err(QueryError::not_impl("BinaryOperator::PGRegexNotMatch")),
                BinaryOperator::PGRegexNotIMatch => Err(QueryError::not_impl("BinaryOperator::PGRegexNotIMatch")),
            }
        }
        Expr::UnaryOp { op, expr } => {
            let val = eval_expr(expr, ctx)?;
            match op {
                UnaryOperator::Plus => Err(QueryError::not_impl("UnaryOperator::Plus")),
                UnaryOperator::Minus => Err(QueryError::not_impl("UnaryOperator::Minus")),
                UnaryOperator::Not => Ok(ParsedValue::BoolVal(!val.as_bool().unwrap_or(false))),
                UnaryOperator::PGBitwiseNot => Err(QueryError::not_impl("UnaryOperator::PGBitwiseNot")),
                UnaryOperator::PGSquareRoot => Err(QueryError::not_impl("UnaryOperator::PGSquareRoot")),
                UnaryOperator::PGCubeRoot => Err(QueryError::not_impl("UnaryOperator::PGCubeRoot")),
                UnaryOperator::PGPostfixFactorial => Err(QueryError::not_impl("UnaryOperator::PGPostfixFactorial")),
                UnaryOperator::PGPrefixFactorial => Err(QueryError::not_impl("UnaryOperator::PGPrefixFactorial")),
                UnaryOperator::PGAbs => Err(QueryError::not_impl("UnaryOperator::PGAbs")),
            }
        }
        Expr::Cast { .. } => Err(QueryError::not_impl("Expr::Cast")),
        Expr::TryCast { .. } => Err(QueryError::not_impl("Expr::TryCast")),
        Expr::Extract { .. } => Err(QueryError::not_impl("Expr::Extract")),
        Expr::Substring { .. } => Err(QueryError::not_impl("Expr::Substring")),
        Expr::Trim { .. } => Err(QueryError::not_impl("Expr::Trim")),
        Expr::Collate { .. } => Err(QueryError::not_impl("Expr::Collate")),
        Expr::Nested(_) => Err(QueryError::not_impl("Expr::Nested")),
        Expr::Value(v) => {
            match v {
                Value::Number(x, _) => {
                    let s : &String = x;
                    if s.find('.').is_some() {
                        str2val(s, &ParsedValueType::DoubleType).ok_or(
                            QueryError::unexpected(
                                &["Failed to parse double value: ", s].join("")
                            )
                        )
                    } else {
                        str2val(s, &ParsedValueType::LongType).ok_or(
                            QueryError::unexpected(
                                &["Failed to parse long value: ", s].join("")
                            )
                        )
                    }
                },
                //Value::Number(x, _) => {}
                Value::SingleQuotedString(x) => {
                    let s: &String = x;
                    Ok(ParsedValue::StrVal(Rc::new(s.clone())))
                },
                Value::NationalStringLiteral(_) => Err(QueryError::not_impl("Value::NationalStringLiteral")),
                Value::HexStringLiteral(_) => Err(QueryError::not_impl("Value::HexStringLiteral")),
                Value::DoubleQuotedString(x) => {
                    let s: &String = x;
                    Ok(ParsedValue::StrVal(Rc::new(s.clone())))
                },
                Value::Boolean(b) => Ok(ParsedValue::BoolVal(*b)),
                Value::Interval { .. } => Err(QueryError::not_impl("Value::Interval")),
                Value::Null => Ok(ParsedValue::NullVal),
                Value::Placeholder(_) => Err(QueryError::not_impl("Value::Placeholder")),
                #[allow(unreachable_patterns)] // XXX the IntelliJ IDE does not see this as unreachable
                _ => Err(QueryError::not_supported("Impossible")),
            }
        }
        Expr::TypedString { .. } => Err(QueryError::not_impl("Expr::TypedString")),
        Expr::MapAccess { .. } => Err(QueryError::not_impl("Expr::MapAccess")),
        Expr::Function(f) => {
            eval_function(f, ctx)
        },
        Expr::Case { .. } => Err(QueryError::not_impl("Expr::Case")),
        Expr::Exists(_) => Err(QueryError::not_impl("Expr::Exists")),
        Expr::Subquery(_) => Err(QueryError::not_impl("Expr::Subquery")),
        Expr::ListAgg(_) => Err(QueryError::not_impl("Expr::ListAgg")),
        Expr::GroupingSets(_) => Err(QueryError::not_impl("Expr::GroupingSets")),
        Expr::Cube(_) => Err(QueryError::not_impl("Expr::Cube")),
        Expr::Rollup(_) => Err(QueryError::not_impl("Expr::Rollup")),
        Expr::Tuple(_) => Err(QueryError::not_impl("Expr::Tuple")),
        Expr::ArrayIndex { .. } => Err(QueryError::not_impl("Expr::ArrayIndex")),
        Expr::Array(_) => Err(QueryError::not_impl("Expr::Array")),
    }
}

fn eval_integer_expr(expr: &Expr, ctx: Option<&ParsedData>, name: &str) -> Result<i64,QueryError> {
  match eval_expr(expr, ctx)? {
      ParsedValue::LongVal(x) => Ok(x),
      x => Err(QueryError::new(&format!(
          "Expression did not evauluate to an integer number ({}): {:?} , expr: {:?}", name, x, expr
      )))
  }
}

pub fn process_query_one_shot(
    schema: &GrokSchema,
    qry: &SqlSelectQuery,
    input: impl Iterator<Item = ParsedMessage>,
) -> Result<ResultTable, QueryError> {
    let mut ret = ResultTable::new();
    let wh: &Expr = qry
        .get_select()
        .selection
        .as_ref()
        .ok_or(QueryError::new("missing where clause"))?;
    let limit = if qry.get_limit().is_some() {
        let num = eval_integer_expr(qry.get_limit().unwrap(), None, "limit")?;
        Some(num as usize)
    } else { None };
    let mut offset = if qry.get_offset().is_some() {
        let num = eval_integer_expr(qry.get_offset().unwrap(), None, "offset")?;
        num
    } else { 0 };
    //let ord = qry.get_order_by();
    //println!("DEBUG QRY: {:?}", &wh);
    for pm in input {
        // let selection: Vec<SelectItem> = qry.get_select().projection;
        // let res_cols = selection.iter().flat_map(|x|{
        //     match x {
        //         SelectItem::UnnamedExpr(ex) => {
        //             let res = eval_expr(ex, Some(pm.get_parsed()))?;
        //             [res.to_rc_str()]
        //         }
        //         SelectItem::ExprWithAlias { expr, alias } => {
        //             []
        //         }
        //         SelectItem::QualifiedWildcard(_) => {}
        //         SelectItem::Wildcard => {
        //
        //         }
        //     }
        // }).collect::<Vec<_>>();
        let result = eval_expr(wh, Some(pm.get_parsed()))?;
        //println!("DEBUG EVAL RESULT: {:?} {:?}", &result, pm.get_parsed());
        if result.as_bool().unwrap_or(false) {
            if offset <= 0 {
                ret.add_row(pm);
                if let Some(lmt) = limit {
                    if ret.get_rows().len() >= lmt {
                        break
                    }
                }
            } else {
                offset -= 1
            }
        }
    }
    //let sort_by = &qry.get_select().sort_by;

    Ok(ret)
}

// pub fn process_query(
//     schema: &GrokSchema,
//     qry: &SqlSelectQuery,
//     input: impl Iterator<Item = ParsedMessage>,
// ) -> Result<ResultTable, QueryError> {
//     Err(QueryError::not_impl("process_query"))
// }

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::rc::Rc;
    use sqlparser::ast::Expr::*;
    use sqlparser::ast::Value::*;
    use sqlparser::ast::{Ident, BinaryOperator::*};
    use crate::{ParsedData, ParsedValue};
    use crate::query_processor::eval_expr;

    #[test]
    fn test_eval_expr() {
        let expr = Box::new(BinaryOp {
                 left: Box::new(BinaryOp {
                     left: Box::new(Identifier(Ident { value: "a".to_string(), quote_style: None })),
                     op: Gt,
                     right: Box::new(Identifier(Ident { value: "b".to_string(), quote_style: None })),
                 }),
                 op: And,
                 right: Box::new(BinaryOp {
                     left: Box::new(Identifier(Ident { value: "b".to_string(), quote_style: None })),
                     op: Lt,
                     right: Box::new(Value(Number("100".to_string(), false)))
                 })
             }
        );
        let hm = HashMap::from(
            [
                (Rc::new("a".to_string()), ParsedValue::LongVal(150)),
                 (Rc::new("b".to_string()), ParsedValue::LongVal(50)),
            ]);
        let pd1 = ParsedData::new(hm);
        let ret = eval_expr(&expr,Some(&pd1)).unwrap();
        println!("RESULT: {:?}", ret);
    }
}
