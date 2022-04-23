// Copyright 2022 Asen Lazarov

use std::error::Error;
use std::fmt;
use chrono::{DateTime, FixedOffset};
use crate::parser::ParsedValue;

#[derive(Debug)]
pub enum FilterError {
    NotImplemented,
    IncompleteExpression,
}

impl fmt::Display for FilterError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Filter error: {}", self)
    }
}

impl Error for FilterError {}

pub enum FilterOp {
    Eq,
    Lt,
    Gt,
    Lte,
    Gte,
    In,
    Between,
    Like,
    ILike,
    RxLike,
}

pub struct FilterOperation {
    col_name: String,
    op: FilterOp,
    operands: Vec<ParsedValue>,
}

fn parse_operation(s: &str) -> Result<(FilterOperation,&str), Box<dyn Error>> {
    Err(Box::new(FilterError::NotImplemented))
}

pub enum FilterToken {
    OpenParen,
    CloseParen,
    And,
    Or,
    Not,
    Operation(FilterOperation),
}

fn parse_token(s: &str) -> Result<(FilterToken,&str), Box<dyn Error>> {
    let s = s.trim_start();
    if (s.is_empty()) {
        return Err(Box::new(FilterError::IncompleteExpression))
    }
    let first_char = s.chars().next().unwrap();
    let token_opt = match first_char {
        '(' => Some(FilterToken::OpenParen),
        ')' => Some(FilterToken::CloseParen),
        '&' => Some(FilterToken::And),
        '|' => Some(FilterToken::Or),
        '!' => Some(FilterToken::Not),
        _ => None
    };
    let (token, s) = if token_opt.is_some() {
        let mut ss = s.chars();
        ss.next();
        (token_opt.unwrap(), ss.as_str())
    } else {
        let (op, s) = parse_operation(s)?;
        (FilterToken::Operation(op), s)
    };
    Ok((token,s))
}

pub struct FilterExpr {
    expr: Vec<FilterToken>,
}

impl FilterExpr {

    pub fn parse(s: &str) -> Result<FilterExpr, Box<dyn Error>> {
        let mut s = s.trim_start();
        let mut tokens = Vec::new();
        while !s.is_empty() {
            let (tkn, ss) = parse_token(s)?;
            tokens.push(tkn);
            s = ss;
        }
        Ok(FilterExpr { expr: tokens })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_works() {
        let parsed =
            FilterExpr::parse("(message like 'ASI%' OR message like 'error')");
    }
}