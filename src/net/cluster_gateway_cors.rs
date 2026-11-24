// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Cross-origin resource sharing: the bucket's CORS rules ([`crate::bucket_config::CorsRule`]) are applied to the
//! requests a browser makes.
//!
//! A request with an `Origin` header to a bucket that has rules is answered with the headers of the first rule
//! that allows its origin and method. A preflight (`OPTIONS` with `Access-Control-Request-Method`) is answered
//! with those headers and an empty body, or refused with 403 when no rule allows it. A request without an
//! `Origin` header costs nothing here: the rules are not read for it.
use super::*;
use crate::bucket_config::{origin_matches, CorsRule};

/// The bucket a path-style request names, if any.
fn bucket_of(request: &Request) -> Option<&str> {
    let bucket = request.path.trim_start_matches('/').split('/').next()?;
    (!bucket.is_empty()).then_some(bucket)
}

/// The first rule that allows `origin` to make `method` with every one of `headers`.
fn rule_for<'a>(rules: &'a [CorsRule], origin: &str, method: &str, headers: &[String]) -> Option<&'a CorsRule> {
    rules.iter().find(|rule| {
        rule.allowed_origins.iter().any(|pattern| origin_matches(pattern, origin))
            && rule.allowed_methods.iter().any(|allowed| allowed.eq_ignore_ascii_case(method))
            && headers
                .iter()
                .all(|wanted| {
                    let wanted = wanted.to_ascii_lowercase();
                    rule.allowed_headers.iter().any(|allowed| origin_matches(&allowed.to_ascii_lowercase(), &wanted))
                })
    })
}

/// What the answer says about the origin: `*` when the rule allows every origin, else the origin itself.
fn allow_origin(rule: &CorsRule, origin: &str) -> String {
    if rule.allowed_origins.iter().any(|pattern| pattern == "*") {
        "*".to_string()
    } else {
        origin.to_string()
    }
}

impl ClusterGateway {
    fn cors_rules(&self, request: &Request) -> Option<std::sync::Arc<crate::bucket_config::BucketConfig>> {
        let rules = self.bucket_rules(bucket_of(request)?).ok().flatten()?;
        (!rules.cors.is_empty()).then_some(rules)
    }

    /// The answer to a preflight, or `None` when the request is not one.
    pub(super) fn cors_preflight(&self, request: &Request) -> Option<Response> {
        if request.method != "OPTIONS" {
            return None;
        }
        let origin = request.header("origin")?;
        let method = request.header("access-control-request-method")?;
        let wanted: Vec<String> = request
            .header("access-control-request-headers")
            .map(|list| list.split(',').map(|name| name.trim().to_string()).filter(|name| !name.is_empty()).collect())
            .unwrap_or_default();
        let refused = || {
            s3::error_response(
                s3::S3ErrorCode::AccessDenied,
                "CORSResponse: this CORS request is not allowed: the bucket has no rule for this origin, method and headers",
                &request.path,
            )
        };
        let rules = match self.cors_rules(request) {
            Some(rules) => rules,
            None => return Some(refused()),
        };
        let Some(rule) = rule_for(&rules.cors, origin, method, &wanted) else {
            return Some(refused());
        };
        let mut response = Response::status(200)
            .with_header("access-control-allow-origin", allow_origin(rule, origin))
            .with_header("access-control-allow-methods", rule.allowed_methods.join(", "))
            .with_header("vary", "Origin, Access-Control-Request-Headers, Access-Control-Request-Method");
        if !wanted.is_empty() {
            response = response.with_header("access-control-allow-headers", wanted.join(", "));
        }
        if rule.max_age_seconds > 0 {
            response = response.with_header("access-control-max-age", rule.max_age_seconds.to_string());
        }
        if !rule.expose_headers.is_empty() {
            response = response.with_header("access-control-expose-headers", rule.expose_headers.join(", "));
        }
        Some(response)
    }

    /// Add the CORS headers a rule allows to the answer to an ordinary cross-origin request.
    pub(super) fn cors_headers(&self, request: &Request, response: &mut Response) {
        let Some(origin) = request.header("origin") else {
            return;
        };
        let Some(rules) = self.cors_rules(request) else {
            return;
        };
        let Some(rule) = rule_for(&rules.cors, origin, &request.method, &[]) else {
            return;
        };
        response.headers.push(("access-control-allow-origin".into(), allow_origin(rule, origin)));
        response.headers.push(("vary".into(), "Origin".into()));
        if !rule.expose_headers.is_empty() {
            response.headers.push(("access-control-expose-headers".into(), rule.expose_headers.join(", ")));
        }
    }
}
