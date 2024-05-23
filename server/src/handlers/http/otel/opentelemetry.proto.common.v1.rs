/*
 * Parseable Server (C) 2022 - 2024 Parseable, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

 // This file was generated by protoc-gen-rust-protobuf. The file was edited after the generation.
 // All the repeated fields were changed to Option<Vec<T>> and the `oneof` fields were changed to Option<T>.

 use serde::{Deserialize, Serialize};
 #[derive(Serialize, Deserialize, Debug, Clone)]
 /// AnyValue is used to represent any type of attribute value. AnyValue may contain a
 /// primitive value such as a string or integer or it may contain an arbitrary nested
 /// object containing arrays, key-value lists and primitives.
 pub struct AnyValue {
     /// The value is one of the listed fields. It is valid for all values to be unspecified
     /// in which case this AnyValue is considered to be "empty".
     pub value: Value,
 }
 
 #[derive(Serialize, Deserialize, Debug, Clone)]
 pub struct Value {
     #[serde(rename = "stringValue")]
     pub str_val: Option<String>,
     #[serde(rename = "boolValue")]
     pub bool_val: Option<bool>,
     #[serde(rename = "intValue")]
     pub int_val: Option<i64>,
     #[serde(rename = "doubleValue")]
     pub double_val: Option<f64>,
     #[serde(rename = "arrayValue")]
     pub array_val: Option<ArrayValue>,
     #[serde(rename = "keyVauleList")]
     pub kv_list_val: Option<KeyValueList>,
     #[serde(rename = "bytesValue")]
     pub bytes_val: Option<String>,
 }
 
 #[derive(Serialize, Deserialize, Debug, Clone)]
 /// ArrayValue is a list of AnyValue messages. We need ArrayValue as a message
 /// since oneof in AnyValue does not allow repeated fields.
 pub struct ArrayValue {
     /// Array of values. The array may be empty (contain 0 elements).
     pub values: Vec<Value>,
 }
 
 #[derive(Serialize, Deserialize, Debug, Clone)]
 /// KeyValueList is a list of KeyValue messages. We need KeyValueList as a message
 /// since `oneof` in AnyValue does not allow repeated fields. Everywhere else where we need
 /// a list of KeyValue messages (e.g. in Span) we use `repeated KeyValue` directly to
 /// avoid unnecessary extra wrapping (which slows down the protocol). The 2 approaches
 /// are semantically equivalent.
 pub struct KeyValueList {
     /// A collection of key/value pairs of key-value pairs. The list may be empty (may
     /// contain 0 elements).
     /// The keys MUST be unique (it is not allowed to have more than one
     /// value with the same key).
     pub values: Vec<KeyValue>,
 }
 
 #[derive(Serialize, Deserialize, Debug, Clone)]
 /// KeyValue is a key-value pair that is used to store Span attributes, Link
 /// attributes, etc.
 pub struct KeyValue {
     pub key: String,
     pub value: Option<Value>,
 }
 
 #[derive(Serialize, Deserialize, Debug)]
 /// InstrumentationScope is a message representing the instrumentation scope information
 /// such as the fully qualified name and version.
 pub struct InstrumentationScope {
     /// An empty instrumentation scope name means the name is unknown.
     pub name: Option<String>,
     pub version: Option<String>,
     /// Additional attributes that describe the scope. \[Optional\].
     /// Attribute keys MUST be unique (it is not allowed to have more than one
     /// attribute with the same key).
     pub attributes: Option<Vec<KeyValue>>,
     #[serde(rename = "droppedAttributesCount")]
     pub dropped_attributes_count: Option<u32>,
 }
 