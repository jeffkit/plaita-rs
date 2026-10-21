//! Property — the input/output data schema of flows and nodes, with the
//! plaita `io.match` strict validation model (no cast coercion; empty
//! strings fail `string`; `max/min` are metadata only).

use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum DataType {
    #[serde(rename = "string")]
    String,
    #[serde(rename = "integer")]
    Integer,
    #[serde(rename = "float")]
    Float,
    #[serde(rename = "bool")]
    Bool,
    #[serde(rename = "object")]
    Object,
    #[serde(rename = "array")]
    Array,
    #[serde(rename = "any")]
    Any,
    #[serde(rename = "number")]
    Number,
}

impl DataType {
    pub fn as_str(&self) -> &'static str {
        match self {
            DataType::String => "string",
            DataType::Integer => "integer",
            DataType::Float => "float",
            DataType::Bool => "bool",
            DataType::Object => "object",
            DataType::Array => "array",
            DataType::Any => "any",
            DataType::Number => "number",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Property {
    #[serde(default)]
    pub data_type: Option<DataType>,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub desc: String,
    #[serde(default)]
    pub default_value: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub properties: Option<Box<indexmap::IndexMap<String, Property>>>,
    #[serde(default)]
    pub is_required: bool,
    #[serde(default)]
    pub max_length: usize,
    #[serde(default)]
    pub max: Option<f64>,
    #[serde(default)]
    pub min: Option<f64>,
    #[serde(default)]
    pub strict: bool,
    /// Array item type: either one property or a list of properties. The
    /// engine validation uses the first entry (Go ItemProperty()).
    #[serde(default, rename = "itemType")]
    pub item_type: Option<Box<Property>>,
    #[serde(default, rename = "items")]
    pub items: Option<Vec<Property>>,
}

impl Property {
    /// `ValidateValue` — `None` data type accepts everything (unset schema).
    pub fn validate_value(&self, value: &Value) -> Result<(), String> {
        let Some(dt) = self.data_type else {
            return Ok(());
        };
        if value.is_null() {
            if self.is_required {
                return Err("value is required but not provided".to_string());
            }
            return Ok(());
        }
        if !self.strict {
            // The loader marks every property strict; flows built directly in
            // Rust (not via ir::load) keep the lenient historical model, which
            // for the JSON value domain differs only on string coercion.
            return self.validate_lenient(dt, value);
        }
        self.validate_strict(dt, value)
    }

    fn validate_lenient(&self, dt: DataType, value: &Value) -> Result<(), String> {
        match dt {
            DataType::String => {
                let s = match value {
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => b.to_string(),
                    _ => return Err(type_mismatch(value, "string")),
                };
                self.validate_string_value(&s)
            }
            DataType::Integer => {
                let ok = matches!(value, Value::Number(_));
                if !ok {
                    return Err(type_mismatch(value, "integer"));
                }
                Ok(())
            }
            DataType::Float | DataType::Number => {
                if !matches!(value, Value::Number(_)) {
                    return Err(type_mismatch(value, dt.as_str()));
                }
                Ok(())
            }
            DataType::Bool => {
                if !matches!(value, Value::Bool(_)) {
                    return Err(type_mismatch(value, "bool"));
                }
                Ok(())
            }
            DataType::Object => match value {
                Value::Object(o) => self.validate_object_values(o),
                _ => Err(type_mismatch(value, "object")),
            },
            DataType::Array => match value {
                Value::Array(a) => self.validate_array_values(a),
                _ => Err(type_mismatch(value, "array")),
            },
            DataType::Any => Ok(()),
        }
    }

    fn validate_strict(&self, dt: DataType, value: &Value) -> Result<(), String> {
        match dt {
            DataType::Any => Ok(()),
            DataType::String => match value {
                Value::String(s) => self.validate_string_value(s),
                _ => Err(type_mismatch(value, "string")),
            },
            DataType::Integer => {
                // JSON cannot distinguish 5 from 5.0 for a Go float64; here
                // serde_json preserves it: only i64/u64 numbers are ints.
                match value {
                    Value::Number(n) if n.is_i64() || n.is_u64() => Ok(()),
                    _ => Err(type_mismatch(value, "integer")),
                }
            }
            DataType::Float | DataType::Number => match value {
                Value::Number(_) => Ok(()),
                _ => Err(type_mismatch(value, dt.as_str())),
            },
            DataType::Bool => {
                if matches!(value, Value::Bool(_)) {
                    Ok(())
                } else {
                    Err(type_mismatch(value, "bool"))
                }
            }
            DataType::Object => match value {
                Value::Object(o) => self.validate_object_values(o),
                _ => Err(type_mismatch(value, "object")),
            },
            DataType::Array => match value {
                Value::Array(a) => self.validate_array_values(a),
                _ => Err(type_mismatch(value, "array")),
            },
        }
    }

    fn validate_string_value(&self, s: &str) -> Result<(), String> {
        if self.max_length > 0 && s.len() > self.max_length {
            return Err(format!(
                "expected max string length is {} but got {}",
                self.max_length,
                s.len()
            ));
        }
        // plaita match: isinstance(obj, str) and obj — empty strings fail
        if s.is_empty() {
            return Err("string value must not be empty".to_string());
        }
        Ok(())
    }

    fn validate_array_values(&self, values: &[Value]) -> Result<(), String> {
        let item_prop = self.item_property();
        let Some(item_prop) = item_prop else {
            return Err("invalid array item property: undefined".to_string());
        };
        for v in values {
            item_prop
                .validate_value(v)
                .map_err(|e| format!("validate array item: {e}"))?;
        }
        Ok(())
    }

    fn validate_object_values(&self, value: &serde_json::Map<String, Value>) -> Result<(), String> {
        if let Some(props) = &self.properties {
            for (key, prop) in props.iter() {
                let v = value.get(key).cloned().unwrap_or(Value::Null);
                prop.validate_value(&v)
                    .map_err(|e| format!("invalid value for property {key:?}: {e}"))?;
            }
        }
        Ok(())
    }

    fn item_property(&self) -> Option<&Property> {
        if let Some(p) = &self.item_type {
            if p.data_type.is_some() {
                return Some(p);
            }
        }
        self.items.as_ref().and_then(|v| v.first())
    }
}

fn type_mismatch(value: &Value, expected: &str) -> String {
    format!(
        "expected value data type is {:?} but got type: {}, value: {}",
        expected,
        json_type_name(value),
        value
    )
}

fn json_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "NoneType",
        Value::Bool(_) => "bool",
        Value::Number(n) if n.is_f64() => "float64",
        Value::Number(_) => "int",
        Value::String(_) => "string",
        Value::Array(_) => "[]interface {}",
        Value::Object(_) => "map[string]interface {}",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn prop(dt: &str) -> Property {
        serde_json::from_value(json!({"dataType": dt, "strict": true})).unwrap()
    }

    #[test]
    fn strict_model() {
        assert!(prop("string").validate_value(&json!("x")).is_ok());
        assert!(
            prop("string").validate_value(&json!("")).is_err(),
            "empty strings fail"
        );
        assert!(prop("string").validate_value(&json!(1)).is_err());
        assert!(prop("integer").validate_value(&json!(5)).is_ok());
        assert!(
            prop("integer").validate_value(&json!(5.0)).is_err(),
            "serde_json keeps 5.0 a float"
        );
        assert!(prop("float").validate_value(&json!(5.0)).is_ok());
        assert!(prop("float").validate_value(&json!(5)).is_ok());
        assert!(prop("float").validate_value(&json!(true)).is_err());
        assert!(prop("bool").validate_value(&json!(true)).is_ok());
        assert!(prop("bool").validate_value(&json!(1)).is_err());
        assert!(prop("any").validate_value(&json!(null)).is_ok());
        assert!(prop("number").validate_value(&json!(1.5)).is_ok());
        assert!(prop("object").validate_value(&json!({})).is_ok());
        assert!(
            prop("array").validate_value(&json!([1])).is_err(),
            "no item property defined"
        );
        let mut p: Property = serde_json::from_value(json!({
            "dataType": "array", "strict": true,
            "itemType": {"dataType": "integer", "strict": true}
        }))
        .unwrap();
        p.strict = true;
        assert!(p.validate_value(&json!([1, 2])).is_ok());
        assert!(p.validate_value(&json!([1, "x"])).is_err());
    }
}
