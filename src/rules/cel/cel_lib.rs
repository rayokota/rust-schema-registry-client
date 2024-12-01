use cel_interpreter::extractors::This;
use cel_interpreter::Context;
use email_address::EmailAddress;
use fluent_uri::{Uri, UriRef};
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

pub fn default_context<'a>() -> Context<'a> {
    let mut context = Context::default();
    context.add_function("isHostname", is_hostname);
    context.add_function("isEmail", is_email);
    context.add_function("isIpv4", is_ipv4);
    context.add_function("isIpv6", is_ipv6);
    context.add_function("isUri", is_uri);
    context.add_function("isUriRef", is_uri_ref);
    context.add_function("isUuid", is_uuid);
    context
}

fn is_hostname(This(s): This<Arc<String>>) -> bool {
    hostname_validator::is_valid(s.as_str())
}

fn is_email(This(s): This<Arc<String>>) -> bool {
    EmailAddress::is_valid(s.as_str())
}

fn is_ipv4(This(s): This<Arc<String>>) -> bool {
    IpAddr::from_str(s.as_str()).is_ok_and(|i| i.is_ipv4())
}

fn is_ipv6(This(s): This<Arc<String>>) -> bool {
    IpAddr::from_str(s.as_str()).is_ok_and(|i| i.is_ipv6())
}

fn is_uri(This(s): This<Arc<String>>) -> bool {
    Uri::parse(s.as_str()).is_ok()
}

fn is_uri_ref(This(s): This<Arc<String>>) -> bool {
    UriRef::parse(s.as_str()).is_ok()
}

fn is_uuid(This(s): This<Arc<String>>) -> bool {
    Uuid::parse_str(s.as_str()).is_ok()
}
