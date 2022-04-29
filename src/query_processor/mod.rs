use std::collections::HashMap;
use crate::parser::*;
use crate::query::*;
use sqlparser::ast::{BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, Ident, ObjectName, SelectItem, UnaryOperator, Value};
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

struct StaticCtx<'a> {
    pd: Option<&'a ParsedData>
}

const EMPTY_CTX: StaticCtx = StaticCtx {
    pd: None
};

impl StaticCtx<'_> {

    fn get_value(&self, key: &str) -> Option<ParsedValue> {
        match self.pd {
           Some(pdd) => pdd.get_value(key).map(|x| { x.clone() }),
           None => None
        }
    }

    fn is_none(&self) -> bool { self.pd.is_none() }
}

struct LazyExpr {
    expr: Expr,
    res: Option<Result<Option<ParsedValue>,QueryError>>,
}

impl LazyExpr {
    fn new(expr: &Expr) -> LazyExpr {
        Self {
            expr: expr.clone(),
            res: None
        }
    }

    fn err(qerr: QueryError) -> LazyExpr {
        Self {
            expr: Expr::Value(Value::Null),
            res: Some(Err(qerr))
        }
    }

    fn clone(&self) -> LazyExpr {
        Self {
            expr: self.expr.clone(),
            res: self.res.clone()
        }
    }
}

struct LazyContext {
    hm: HashMap<Rc<str>,LazyExpr>,
}

impl LazyContext {

    fn empty() -> LazyContext {
        Self {
            hm: HashMap::new()
        }
    }

    fn get_value(&mut self, key: &str, ctx: &StaticCtx) -> Result<Option<ParsedValue>,QueryError> {
        let lex_opt = self.hm.get_mut(key);
        if lex_opt.is_none() {
            return Ok(None)
        }
        let lex = lex_opt.unwrap();
        if lex.res.is_some() {
            return lex.res.as_ref().unwrap().clone()
        }
        //TODO FIXME ??? cant't pass self as lazy context
        let pv = eval_expr(&lex.expr, ctx, &mut LazyContext::empty())?;
        lex.res = Some(Ok(Some(pv)));
        return lex.res.as_ref().unwrap().clone()
    }

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

fn func_arg_to_pv(arg: &FunctionArg, ctx: &StaticCtx, dctx: &mut LazyContext) -> Result<ParsedValue, QueryError> {
    match arg {
        FunctionArg::Named { .. } => {
            Err(QueryError::not_supported("Named function arguments are not supported yet"))
        }
        FunctionArg::Unnamed(fax) => {
            match fax {
                FunctionArgExpr::Expr(xp) => {
                    eval_expr(xp, ctx, dctx)
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

fn eval_function_date(fun: &Function, ctx: &StaticCtx, dctx: &mut LazyContext) -> Result<ParsedValue, QueryError> {
    let mut args_iter = fun.args.iter();
    let cur_arg: &FunctionArg = args_iter.next().ok_or(QueryError::new("DATE function requires arguments"))?;
    let curv = func_arg_to_pv(cur_arg, ctx, dctx)?;
    let tformat = if let ParsedValue::StrVal(rs) = curv {
        Ok(TimeTypeFormat::new(rs.as_str()))
    } else { Err(QueryError::new("DATE function first argument must be a time format string")) }?;
    let cur_arg: &FunctionArg = args_iter.next().ok_or(QueryError::new("DATE function requires arguments"))?;
    let curv = func_arg_to_pv(cur_arg, ctx, dctx)?;
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

fn eval_function(fun: &Function, ctx: &StaticCtx, dctx: &mut LazyContext) -> Result<ParsedValue, QueryError> {
    let fun_name = object_name_to_string(&fun.name);
    match fun_name.as_str() {
        "DATE" => eval_function_date(fun, ctx, dctx),
        _ => Err(QueryError::not_supported(&format!("Function: {:?}", fun)))
    }
}

fn eval_expr(expr: &Expr, ctx: &StaticCtx, dctx: &mut LazyContext) -> Result<ParsedValue, QueryError> {
    match expr {
        Expr::Identifier(x) => {
            if x.quote_style.is_some() || ctx.is_none() {
                Ok(ParsedValue::StrVal(Rc::new(x.value.clone())))
            } else {
                if let Some(lazyv) = dctx.get_value(&x.value, ctx)? {
                    Ok(lazyv)
                } else if let Some(val) = ctx.get_value(&x.value) {
                    Ok(val.clone())
                } else {
                    Ok(ParsedValue::NullVal)
                }
            }
        }
        Expr::CompoundIdentifier(_) => Err(QueryError::not_impl("Expr::CompoundIdentifier")),
        Expr::IsNull(x) => {
            let res = eval_expr(x, ctx, dctx)?;
            Ok(ParsedValue::BoolVal(res == ParsedValue::NullVal))
        }
        Expr::IsNotNull(x) => {
            let res = eval_expr(x, ctx, dctx)?;
            Ok(ParsedValue::BoolVal(res != ParsedValue::NullVal))
        }
        Expr::IsDistinctFrom(_, _) => Err(QueryError::not_impl("Expr::IsDistinctFrom")),
        Expr::IsNotDistinctFrom(_, _) => Err(QueryError::not_impl("Expr::IsNotDistinctFrom")),
        Expr::InList { .. } => Err(QueryError::not_impl("Expr::InList")),
        Expr::InSubquery { .. } => Err(QueryError::not_impl("Expr::InSubquery")),
        Expr::InUnnest { .. } => Err(QueryError::not_impl("Expr::InUnnest")),
        Expr::Between { .. } => Err(QueryError::not_impl("Expr::Between")),
        Expr::BinaryOp { left, op, right } => {
            let lval = eval_expr(left, ctx, dctx)?;
            let mut rval = || { // using a closure for lazy evaluation
                let rv = eval_expr(right, ctx, dctx)?;
                Ok(rv)
            };
            // another closure for lazy eval
            let mut arithmetic_op = |op: &BinaryOperator| {
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
            let val = eval_expr(expr, ctx, dctx)?;
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
            eval_function(f, ctx, dctx)
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

fn eval_integer_expr(expr: &Expr, ctx: &StaticCtx, dctx: &mut LazyContext, name: &str) -> Result<i64,QueryError> {
  match eval_expr(expr, ctx, dctx)? {
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
    let mut empty_lazy_context = LazyContext::empty();
    let limit = if qry.get_limit().is_some() {
        let num = eval_integer_expr(qry.get_limit().unwrap(), &EMPTY_CTX,
                                    &mut empty_lazy_context, "limit")?;
        Some(num as usize)
    } else { None };
    let mut offset = if qry.get_offset().is_some() {
        let num = eval_integer_expr(qry.get_offset().unwrap(), &EMPTY_CTX,
                                    &mut empty_lazy_context, "offset")?;
        num
    } else { 0 };
    //let ord = qry.get_order_by();
    //println!("DEBUG QRY: {:?}", &wh);

    let selection: &Vec<SelectItem> = &qry.get_select().projection;
    let res_cols: Vec<(Rc<str>, LazyExpr)> = selection.iter().enumerate().flat_map(|(i, x)|{
        match x {
            SelectItem::UnnamedExpr(expr) => {
                let my_name = i.to_string();
                vec![(
                    Rc::from(my_name.as_str()),
                    LazyExpr::new(expr)
                    )]
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                vec![(
                    Rc::from(alias.value.as_str()),
                    LazyExpr::new(expr)
                )]
            }
            SelectItem::QualifiedWildcard(wc) => {
                vec![(
                    Rc::from( object_name_to_string(wc).as_str()),
                    LazyExpr::err(QueryError::not_supported("SelectItem::QualifiedWildcard"))
                )]
            }
            SelectItem::Wildcard => {
                let vec = schema.columns().iter().map(|cd| {
                    let col_name = cd.col_name().as_str();
                    (
                        Rc::from(col_name),
                        LazyExpr::new(&Expr::Identifier(Ident::new(col_name)))
                        )
                }).collect::<Vec<_>>();
                vec
            }
        }
    }).collect::<Vec<_>>();

    for pm in input {
        let static_ctx = StaticCtx {
            pd: Some(pm.get_parsed()),
        };
        let mut dctx = LazyContext {
            hm: res_cols.iter().map(|(k,v)| {
                (k.clone(), v.clone())
            }).collect()
        };
        let result = eval_expr(wh, &static_ctx, &mut dctx)?;
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
    use crate::query_processor::{eval_expr, LazyContext, StaticCtx};

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
                (Rc::from("a"), ParsedValue::LongVal(150)),
                 (Rc::from("b"), ParsedValue::LongVal(50)),
            ]);
        let pd1 = ParsedData::new(hm);
        let ret = eval_expr(&expr,&StaticCtx{ pd: Some(&pd1) }, &mut LazyContext::empty()).unwrap();
        println!("RESULT: {:?}", ret);
    }
}
