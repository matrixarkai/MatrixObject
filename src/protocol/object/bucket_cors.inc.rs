// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BucketCorsPolicy {
    pub allowed_origins: Vec<String>,
    pub allowed_methods: Vec<String>,
    pub allowed_headers: Vec<String>,
    pub expose_headers: Vec<String>,
    pub max_age_seconds: u64,
}

impl BucketCorsPolicy {
    pub fn disabled(&self) -> bool {
        self.allowed_origins.is_empty()
            && self.allowed_methods.is_empty()
            && self.allowed_headers.is_empty()
            && self.expose_headers.is_empty()
            && self.max_age_seconds == 0
    }

    pub fn canonicalize(&mut self) {
        canonicalize_string_list(&mut self.allowed_origins);
        canonicalize_string_list(&mut self.allowed_methods);
        canonicalize_string_list(&mut self.allowed_headers);
        canonicalize_string_list(&mut self.expose_headers);
    }

    pub fn effective_with_default_origins(&self, default_allowed_origins: &[String]) -> Self {
        let mut policy = self.clone();
        if policy.allowed_origins.is_empty() {
            policy.allowed_origins = if default_allowed_origins.is_empty() {
                vec!["*".to_string()]
            } else {
                default_allowed_origins.to_vec()
            };
        }
        if policy.allowed_methods.is_empty() {
            policy.allowed_methods = DEFAULT_CORS_ALLOWED_METHODS
                .iter()
                .map(|value| (*value).to_string())
                .collect();
        }
        if policy.allowed_headers.is_empty() {
            policy.allowed_headers = DEFAULT_CORS_ALLOWED_HEADERS
                .iter()
                .map(|value| (*value).to_string())
                .collect();
        }
        if policy.expose_headers.is_empty() {
            policy.expose_headers = DEFAULT_CORS_EXPOSE_HEADERS
                .iter()
                .map(|value| (*value).to_string())
                .collect();
        }
        if policy.max_age_seconds == 0 {
            policy.max_age_seconds = 86_400;
        }
        policy
    }

    pub fn effective(&self) -> Self {
        self.effective_with_default_origins(&[])
    }

    pub fn to_control_fields(&self, field_prefix: &str) -> BTreeMap<String, String> {
        let mut fields = BTreeMap::new();
        append_string_list_control_fields(
            "allowed_origins",
            &self.allowed_origins,
            field_prefix,
            &mut fields,
        );
        append_string_list_control_fields(
            "allowed_methods",
            &self.allowed_methods,
            field_prefix,
            &mut fields,
        );
        append_string_list_control_fields(
            "allowed_headers",
            &self.allowed_headers,
            field_prefix,
            &mut fields,
        );
        append_string_list_control_fields(
            "expose_headers",
            &self.expose_headers,
            field_prefix,
            &mut fields,
        );
        fields.insert(
            format!("{field_prefix}max_age_seconds"),
            self.max_age_seconds.to_string(),
        );
        fields
    }

    pub fn response_allowed_origin(&self, origin: &str) -> Option<String> {
        let trimmed = origin.trim();
        if trimmed.is_empty() {
            return None;
        }
        let policy = self.effective();
        if cors_value_allowed(&policy.allowed_origins, trimmed) {
            Some(trimmed.to_string())
        } else {
            None
        }
    }

    pub fn evaluate_preflight(
        &self,
        request: BucketCorsPreflightRequest,
    ) -> Result<BucketCorsPreflightDecision, ObjectError> {
        let policy = self.effective();
        if let Some(origin) = request.origin.as_deref().map(str::trim) {
            if !origin.is_empty() && !cors_value_allowed(&policy.allowed_origins, origin) {
                return Err(ObjectError::PreconditionFailed(
                    "request origin is not allowed by bucket CORS policy".to_string(),
                ));
            }
        }
        if let Some(method) = request.requested_method.as_deref().map(str::trim) {
            if !method.is_empty() && !cors_value_allowed(&policy.allowed_methods, method) {
                return Err(ObjectError::PreconditionFailed(
                    "requested method is not allowed by bucket CORS policy".to_string(),
                ));
            }
        }
        for header in &request.requested_headers {
            if !cors_value_allowed(&policy.allowed_headers, header) {
                return Err(ObjectError::PreconditionFailed(
                    "requested header is not allowed by bucket CORS policy".to_string(),
                ));
            }
        }
        let allowed_origin = request
            .origin
            .as_deref()
            .map(str::trim)
            .filter(|origin| !origin.is_empty())
            .map(str::to_string);
        let allowed_headers = if request.requested_headers.is_empty() {
            policy.allowed_headers.clone()
        } else {
            request.requested_headers
        };
        Ok(BucketCorsPreflightDecision {
            allowed_origin,
            allowed_methods: policy.allowed_methods,
            allowed_headers,
            expose_headers: policy.expose_headers,
            max_age_seconds: policy.max_age_seconds,
        })
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BucketCorsPreflightRequest {
    pub origin: Option<String>,
    pub requested_method: Option<String>,
    pub requested_headers: Vec<String>,
}

impl BucketCorsPreflightRequest {
    pub fn from_header_values(
        origin: Option<&str>,
        requested_method: Option<&str>,
        requested_headers: Option<&str>,
    ) -> Self {
        Self {
            origin: origin.map(str::to_string),
            requested_method: requested_method.map(str::to_string),
            requested_headers: requested_headers
                .map(split_cors_header_list)
                .unwrap_or_default(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BucketCorsPreflightDecision {
    pub allowed_origin: Option<String>,
    pub allowed_methods: Vec<String>,
    pub allowed_headers: Vec<String>,
    pub expose_headers: Vec<String>,
    pub max_age_seconds: u64,
}

impl BucketCorsPreflightDecision {
    pub fn allowed_methods_header(&self) -> String {
        self.allowed_methods.join(",")
    }

    pub fn allowed_headers_header(&self) -> String {
        self.allowed_headers.join(",")
    }

    pub fn expose_headers_header(&self) -> String {
        self.expose_headers.join(",")
    }
}

pub fn split_cors_header_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_string)
        .collect()
}

pub fn cors_pattern_matches(pattern: &str, value: &str) -> bool {
    let pattern = pattern.trim().to_ascii_lowercase();
    let value = value.trim().to_ascii_lowercase();
    if pattern.is_empty() || value.is_empty() {
        return false;
    }
    if pattern == "*" {
        return true;
    }
    match pattern.find('*') {
        None => pattern == value,
        Some(wildcard) => {
            let prefix = &pattern[..wildcard];
            let suffix = &pattern[wildcard + 1..];
            value.len() >= prefix.len() + suffix.len()
                && value.starts_with(prefix)
                && value.ends_with(suffix)
        }
    }
}

pub fn cors_value_allowed(allowed: &[String], value: &str) -> bool {
    allowed
        .iter()
        .any(|pattern| cors_pattern_matches(pattern, value))
}

fn append_string_list_control_fields(
    name: &str,
    values: &[String],
    field_prefix: &str,
    fields: &mut BTreeMap<String, String>,
) {
    fields.insert(
        format!("{field_prefix}{name}_count"),
        values.len().to_string(),
    );
    for (index, value) in values.iter().enumerate() {
        fields.insert(format!("{field_prefix}{name}.{index}"), value.clone());
    }
}

pub(crate) fn control_fields_checksum(fields: &BTreeMap<String, String>) -> String {
    let mut material = String::new();
    for (key, value) in fields {
        material.push_str(key);
        material.push('\n');
        material.push_str(value);
        material.push('\n');
    }
    etag(material.as_bytes())
}
