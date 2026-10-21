//! plaita expression language: `$`-rooted variables, `$F.fn(...)` calls and
//! `{% ... %}` template interpolation over JSON data with Python value
//! semantics.
//!
//! Semantic authority is plaita's `plaita/core/expression_parser.py` module
//! docstring and `plaita/tests/unit/test_expression_golden.py`; the golden
//! cases are vendored under `testdata/expression_golden.json` and run in
//! `tests/golden.rs`.
//!
//! ```no_run
//! use plaita_expression::{eval_value, MapRoots};
//! use serde_json::json;
//!
//! let mut roots = MapRoots::new();
//! roots.set("INPUT", json!({"name": "plaita"}));
//! let out = eval_value(&json!("hello {%$INPUT.name%}"), &roots).unwrap();
//! assert_eq!(out, json!("hello plaita"));
//! ```

mod error;
mod eval;
mod functions;
mod parser;
mod pyfmt;
mod pyrepr;
mod strftime;
mod value;

pub use error::{ErrorKind, EvalError, ParseError};
pub use eval::{
    compare as py_compare, contains, equal, error_kind, eval_expr, eval_str, eval_value, truthy,
    MapRoots, Roots, BUILTIN_ROOTS,
};
pub use functions::{count as function_count, lookup as lookup_function, names as function_names};
pub use parser::{parse, parse_template, Expr, Segment, TemplatePart};
pub use pyrepr::{py_float_repr, py_repr, py_str};
pub use value::Object;
