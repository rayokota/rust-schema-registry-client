// This file is @generated by prost-build.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(::prost_reflect::ReflectMessage)]
#[prost_reflect(message_name = "test.TestMessage")]
#[prost_reflect(descriptor_pool = "crate::TEST_DESCRIPTOR_POOL")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TestMessage {
    #[prost(string, tag = "1")]
    pub test_string: ::prost::alloc::string::String,
    #[prost(bool, tag = "2")]
    pub test_bool: bool,
    #[prost(bytes = "vec", tag = "3")]
    pub test_bytes: ::prost::alloc::vec::Vec<u8>,
    #[prost(double, tag = "4")]
    pub test_double: f64,
    #[prost(float, tag = "5")]
    pub test_float: f32,
    #[prost(fixed32, tag = "6")]
    pub test_fixed32: u32,
    #[prost(fixed64, tag = "7")]
    pub test_fixed64: u64,
    #[prost(int32, tag = "8")]
    pub test_int32: i32,
    #[prost(int64, tag = "9")]
    pub test_int64: i64,
    #[prost(sfixed32, tag = "10")]
    pub test_sfixed32: i32,
    #[prost(sfixed64, tag = "11")]
    pub test_sfixed64: i64,
    #[prost(sint32, tag = "12")]
    pub test_sint32: i32,
    #[prost(sint64, tag = "13")]
    pub test_sint64: i64,
    #[prost(uint32, tag = "14")]
    pub test_uint32: u32,
    #[prost(uint64, tag = "15")]
    pub test_uint64: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(::prost_reflect::ReflectMessage)]
#[prost_reflect(message_name = "test.DependencyMessage")]
#[prost_reflect(descriptor_pool = "crate::TEST_DESCRIPTOR_POOL")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DependencyMessage {
    #[prost(bool, tag = "1")]
    pub is_active: bool,
    #[prost(message, optional, tag = "2")]
    pub test_message: ::core::option::Option<TestMessage>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(::prost_reflect::ReflectMessage)]
#[prost_reflect(message_name = "test.Author")]
#[prost_reflect(descriptor_pool = "crate::TEST_DESCRIPTOR_POOL")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Author {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(int32, tag = "2")]
    pub id: i32,
    #[prost(bytes = "vec", tag = "3")]
    pub picture: ::prost::alloc::vec::Vec<u8>,
    #[prost(string, repeated, tag = "4")]
    pub works: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(oneof = "author::PiiOneof", tags = "5, 6")]
    pub pii_oneof: ::core::option::Option<author::PiiOneof>,
}
/// Nested message and enum types in `Author`.
pub mod author {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum PiiOneof {
        #[prost(message, tag = "5")]
        OneofMessage(super::Pizza),
        #[prost(string, tag = "6")]
        OneofString(::prost::alloc::string::String),
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(::prost_reflect::ReflectMessage)]
#[prost_reflect(message_name = "test.Pizza")]
#[prost_reflect(descriptor_pool = "crate::TEST_DESCRIPTOR_POOL")]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Pizza {
    #[prost(string, tag = "1")]
    pub size: ::prost::alloc::string::String,
    #[prost(string, repeated, tag = "2")]
    pub toppings: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
