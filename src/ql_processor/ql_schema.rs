use crate::ql_processor::ql_agg_expr::{get_agg_expr, AggExpr, DynAggExpr};
use crate::ql_processor::ql_eval_expr::{
    eval_expr_type, object_name_to_string, LazyContext, LazyExpr,
};
use crate::ql_processor::SqlSelectQuery;
use crate::parser::{
    GrokColumnDef, GrokSchema, ParsedMessage, ParsedValue, ParsedValueType, ParserColDef,
    ParserSchema, RawMessage,
};
use sqlparser::ast::{BinaryOperator, Expr, SelectItem};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
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

    pub fn incompatible_types(
        ltype: &ParsedValue,
        rtype: &ParsedValue,
        op: &BinaryOperator,
    ) -> QueryError {
        QueryError(format!(
            "Incompatible types for op: {:?} lval={:?} rval={:?}",
            op, ltype, rtype
        ))
    }
}
impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Query error: {}", self.0)
    }
}

impl Error for QueryError {}

#[derive(Clone)]
pub struct QlColDef {
    pcd: ParserColDef,
}

impl QlColDef {
    pub fn new(name: &str, pv_type: ParsedValueType) -> Self {
        Self {
            pcd: ParserColDef::new(name, &pv_type),
        }
    }

    pub fn from(gcd: &GrokColumnDef) -> Self {
        Self {
            pcd: ParserColDef::new(gcd.col_name(), gcd.col_type()),
        }
    }

    pub fn name(&self) -> &Arc<str> {
        &self.pcd.name()
    }

    pub fn get_pcd(&self) -> &ParserColDef {
        &self.pcd
    }
}

#[derive(Clone)]
pub struct QlSchema {
    name: Arc<str>,
    cols: Vec<QlColDef>,
}

impl QlSchema {
    pub fn new(name: Arc<str>, cols: Vec<QlColDef>) -> Self {
        Self { name, cols }
    }
    pub fn from(gs: &GrokSchema) -> QlSchema {
        let cols = gs
            .columns()
            .iter()
            .map(|gcd| QlColDef::from(gcd))
            .collect::<Vec<_>>();
        Self {
            name: Arc::from(gs.name()),
            cols,
        }
    }
}

impl ParserSchema for QlSchema {
    fn name(&self) -> &str {
        self.name.as_ref()
    }

    fn col_defs(&self) -> Vec<&ParserColDef> {
        self.cols.iter().map(|cd| cd.get_pcd()).collect()
    }
}

#[derive(Debug, Clone)]
pub struct QlRow {
    raw: Option<RawMessage>,
    data: Vec<(Arc<str>, Arc<ParsedValue>)>,
}

pub type QlRowBatch = Vec<QlRow>;

impl QlRow {
    pub fn new(raw: Option<RawMessage>, data: Vec<(Arc<str>, Arc<ParsedValue>)>) -> Self {
        Self { raw, data }
    }

    pub fn from_parsed_message(pm: ParsedMessage, schema: &QlSchema) -> QlRow {
        let parsed = pm.get_parsed();
        let rdata = schema
            .cols
            .iter()
            .map(|qc| {
                (
                    qc.name().clone(),
                    parsed
                        //TODO - is it possible to consume the values instead of cloning???
                        .get_value(qc.name().as_ref())
                        .map(|x| x.clone())
                        .unwrap_or(Arc::new(ParsedValue::NullVal)),
                )
            })
            .collect::<Vec<_>>();
        Self {
            raw: Some(pm.consume_raw()),
            data: rdata,
        }
    }

    pub fn raw(&self) -> &Option<RawMessage> {
        &self.raw
    }

    pub fn data(&self) -> &Vec<(Arc<str>, Arc<ParsedValue>)> {
        &self.data
    }

    pub fn data_as_strs(&self) -> Vec<Rc<str>> {
        self.data()
            .iter()
            .map(|x| x.1.to_rc_str())
            .collect::<Vec<_>>()
    }
}

pub struct QlRowContext<'a> {
    row: Option<&'a QlRow>,
    lookup_map: HashMap<Arc<str>, Arc<ParsedValue>>,
}

impl<'a> QlRowContext<'a> {
    pub fn from_row(row: &'a QlRow) -> QlRowContext {
        let mut lookup_map: HashMap<Arc<str>, Arc<ParsedValue>> = HashMap::new();
        for (k, v) in &row.data {
            lookup_map.insert(k.clone(), Arc::clone(v));
        }
        Self {
            row: Some(row),
            lookup_map,
        }
    }

    pub fn empty() -> QlRowContext<'a> {
        Self {
            row: None,
            lookup_map: HashMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.row.is_none()
    }

    pub fn get_value(&self, key: &str) -> Option<Arc<ParsedValue>> {
        self.lookup_map.get(key).map(|v| v.clone())
    }
}

pub enum QlSelectItem {
    RawMessage,          // *
    LazyExpr(LazyExpr),  // per row expr, must be cloned per row
    AggExpr(DynAggExpr), // per query expression (aggregate)
}

pub struct QlSelectCols {
    cols: Vec<QlSelectItem>,
}

impl QlSelectCols {
    pub fn new(cols: Vec<QlSelectItem>) -> Self {
        Self { cols }
    }

    pub fn has_raw_message(&self) -> bool {
        self.cols.iter().any(|c| match c {
            QlSelectItem::RawMessage => true,
            QlSelectItem::LazyExpr(_) => false,
            QlSelectItem::AggExpr(_) => false,
        })
    }

    pub fn has_agg_expr(&self) -> bool {
        self.cols.iter().any(|c| match c {
            QlSelectItem::RawMessage => false,
            QlSelectItem::LazyExpr(_) => false,
            QlSelectItem::AggExpr(_) => true,
        })
    }

    pub fn validate_group_by_cols(
        &self,
        group_by_col_ixes: &Vec<usize>,
    ) -> Result<bool, QueryError> {
        let has_agg = self.has_agg_expr();
        let gbe_ixes_set = group_by_col_ixes
            .iter()
            .map(|x| *x)
            .collect::<HashSet<usize>>();
        if !has_agg && !gbe_ixes_set.is_empty() {
            return Err(QueryError::new(
                "GROUP BY requires an aggregate function to be specified",
            ));
        }
        if has_agg {
            for (i, c) in self.cols.iter().enumerate() {
                if let QlSelectItem::LazyExpr(_) = c {
                    if !gbe_ixes_set.contains(&i) {
                        return Err(QueryError::new(
                            "All non-aggregate select expressions must be part of the GROUP BY ",
                        ));
                    }
                }
            }
        }
        Ok(has_agg)
    }

    // pub fn validate_group_by(&self, group_by_exprs: &Vec<&Expr>) -> Result<(), QueryError> {
    //     let mut gbe_set = group_by_exprs.iter()
    //         .map(|e| *e).collect::<HashSet<_>>();
    //     let has_agg = self.has_agg_expr();
    //     if !has_agg && !gbe_set.is_empty() {
    //         return Err(QueryError::new("GROUP BY requires an aggregate function to be specified"))
    //     }
    //     for c in &self.cols {
    //         if let QlSelectItem::LazyExpr(ex) = c {
    //             if !gbe_set.remove(ex.expr()) {
    //                 return Err(QueryError::new(
    //                     "All non aggregate select expressions must be present in GROUP BY clause"))
    //             }
    //         }
    //     }
    //     if !gbe_set.is_empty() {
    //         return Err(QueryError::new(
    //             "All GROUP BY expressions must be part of the SELECT clause"))
    //     }
    //     Ok(())
    // }

    pub fn lazy_exprs(&self) -> Vec<&LazyExpr> {
        let mut ret = Vec::new();
        for c in &self.cols {
            if let QlSelectItem::LazyExpr(lex) = c {
                ret.push(lex);
            }
        }
        ret
    }

    pub fn lazy_context(&self) -> LazyContext {
        let mut hm = HashMap::new();
        for c in &self.cols {
            if let QlSelectItem::LazyExpr(lex) = c {
                hm.insert(lex.name().clone(), lex.clone());
            }
        }
        LazyContext::new(hm)
    }

    pub fn agg_exprs(&self) -> Vec<Box<dyn AggExpr>> {
        let mut ret: Vec<Box<dyn AggExpr>> = Vec::new();
        for c in &self.cols {
            if let QlSelectItem::AggExpr(aex) = c {
                let cl = aex.clone_expr();
                ret.push(cl);
            }
        }
        ret
    }

    pub fn cols(&self) -> &Vec<QlSelectItem> {
        &self.cols
    }

    pub fn to_out_schema(&self, inp_schema: &QlSchema) -> Result<QlSchema, QueryError> {
        let ctx = inp_schema
            .cols
            .iter()
            .map(|qcd| (qcd.name().clone(), qcd.pcd.pv_type().clone()))
            .collect::<HashMap<Arc<str>, ParsedValueType>>();
        let cols = self
            .cols
            .iter()
            .map(|cd| {
                let nt = match cd {
                    QlSelectItem::RawMessage => ("_raw", Ok(ParsedValueType::StrType)),
                    QlSelectItem::LazyExpr(x) => {
                        (x.name().as_ref(), eval_expr_type(x.expr(), &ctx))
                    }
                    QlSelectItem::AggExpr(ax) => (ax.name().as_ref(), ax.result_type(&ctx)),
                };

                (nt.0, nt.1)
            })
            .collect::<Vec<_>>();
        let mut my_cols = Vec::new();
        for (k, v) in cols {
            let col = QlColDef::new(k, v?);
            my_cols.push(col)
        }
        Ok(QlSchema::new(inp_schema.name.clone(), my_cols))
    }
}

fn get_select_expr_name(expr: &Expr) -> Option<String> {
    if let Expr::Identifier(idt) = expr {
        Some(idt.value.clone())
    } else {
        None
    }
}

fn get_res_col(name: &str, expr: &Expr) -> (Arc<str>, QlSelectItem) {
    let my_name = Arc::from(name);
    let agg = get_agg_expr(&my_name, expr);
    match agg {
        Ok(opt) => match opt {
            None => (
                my_name.clone(),
                QlSelectItem::LazyExpr(LazyExpr::new(my_name.clone(), expr)),
            ),
            Some(agg) => (my_name, QlSelectItem::AggExpr(agg)),
        },
        Err(x) => (
            my_name.clone(),
            QlSelectItem::LazyExpr(LazyExpr::err(my_name.clone(), x)),
        ),
    }
}

pub fn get_res_cols(_schema: &GrokSchema, qry: &SqlSelectQuery) -> Vec<QlSelectItem> {
    let selection: &Vec<SelectItem> = &qry.get_select().projection;
    selection
        .iter()
        .enumerate()
        .flat_map(|(i, x)| {
            let i = i + 1;
            match x {
                SelectItem::UnnamedExpr(expr) => {
                    let my_name = get_select_expr_name(expr).unwrap_or(i.to_string());
                    let t = get_res_col(my_name.as_str(), expr);
                    vec![t.1]
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let t = get_res_col(alias.value.as_str(), expr);
                    vec![t.1]
                }
                SelectItem::QualifiedWildcard(wc) => {
                    let my_name: Arc<str> = Arc::from(object_name_to_string(wc).as_str());
                    vec![
                        //(
                        //my_name.clone(),
                        QlSelectItem::LazyExpr(LazyExpr::err(
                            my_name.clone(),
                            QueryError::not_supported("SelectItem::QualifiedWildcard"),
                        )),
                        //)
                    ]
                }
                SelectItem::Wildcard => {
                    // let vec = schema
                    //     .columns()
                    //     .iter()
                    //     .map(|cd| {
                    //         let col_name: Rc<str> = Rc::from(cd.col_name().as_str());
                    //         (
                    //             col_name.clone(),
                    //             QlSelectItem::LazyExpr(LazyExpr::new(
                    //                 col_name.clone(),
                    //                 &Expr::Identifier(Ident::new(col_name.to_string())))),
                    //         )
                    //     })
                    //     .collect::<Vec<_>>();
                    // vec
                    //let my_name = i.to_string();
                    vec![
                        //(
                        //Rc::from(my_name.as_str()),
                        QlSelectItem::RawMessage,
                        //)
                    ]
                }
            }
        })
        .collect::<Vec<_>>()
}
