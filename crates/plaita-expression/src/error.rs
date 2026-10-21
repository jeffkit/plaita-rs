//! Error types of the expression language.
//!
//! Errors carry a Python exception class name so engine-side classification
//! (and humans reading logs) can tell a flow-logic error (`KeyError` on a
//! missing root) from a type misuse (`TypeError`) or a misspelled function
//! (`NameError`). Every variant is deterministic: re-evaluating the same
//! expression against the same context fails the same way, so none of them
//! is worth a retry.

use std::fmt;

/// Position-annotated parse failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("cannot parse expression {source_text:?}: {message} (near column {column})")]
pub struct ParseError {
    pub source_text: String,
    pub column: usize,
    pub message: String,
}

/// Python exception class the evaluation failure maps onto.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// Missing context root / dict key (`KeyError`).
    KeyError,
    /// Sequence index out of range (`IndexError`).
    IndexError,
    /// Operand/argument type misuse (`TypeError`).
    TypeError,
    /// Bad value, incl. plaita#69 mutator write protection (`ValueError`).
    ValueError,
    /// Unknown `$F` function (`NameError`).
    NameError,
    /// Division/modulo by zero (`ZeroDivisionError`).
    ZeroDivisionError,
    /// Non-representable float result (`OverflowError`).
    OverflowError,
    /// Expression text did not parse (`ParseException`).
    ParseError,
    /// Anything else (JSON decoding, formatting, ...).
    Other,
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ErrorKind::KeyError => "KeyError",
            ErrorKind::IndexError => "IndexError",
            ErrorKind::TypeError => "TypeError",
            ErrorKind::ValueError => "ValueError",
            ErrorKind::NameError => "NameError",
            ErrorKind::ZeroDivisionError => "ZeroDivisionError",
            ErrorKind::OverflowError => "OverflowError",
            ErrorKind::ParseError => "ParseException",
            ErrorKind::Other => "Error",
        })
    }
}

/// Evaluation failure: a Python exception class plus message.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{kind}: {message}")]
pub struct EvalError {
    pub kind: ErrorKind,
    pub message: String,
}

impl EvalError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        EvalError {
            kind,
            message: message.into(),
        }
    }
    pub fn key_error(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::KeyError, message)
    }
    pub fn index_error(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::IndexError, message)
    }
    pub fn type_error(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::TypeError, message)
    }
    pub fn value_error(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::ValueError, message)
    }
    pub fn name_error(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::NameError, message)
    }
    pub fn zero_division(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::ZeroDivisionError, message)
    }
    pub fn overflow(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::OverflowError, message)
    }
    pub fn other(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Other, message)
    }
}

impl From<ParseError> for EvalError {
    fn from(e: ParseError) -> Self {
        EvalError::new(ErrorKind::ParseError, e.to_string())
    }
}
