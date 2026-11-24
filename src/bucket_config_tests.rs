// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
use super::*;

fn configured() -> BucketConfig {
    let mut config = BucketConfig::new(1_700_000_000_000);
    config.versioning = VERSIONING_ENABLED;
    config.tags.insert("team".into(), "storage & more".into());
    config.cors.push(CorsRule {
        id: "web".into(),
        allowed_origins: vec!["https://*.example.com".into()],
        allowed_methods: vec!["GET".into(), "PUT".into()],
        allowed_headers: vec!["*".into()],
        expose_headers: vec!["etag".into()],
        max_age_seconds: 600,
    });
    config.policy = "{\"Version\":\"2012-10-17\"}".into();
    config.lifecycle.push(LifecycleRule { id: "old".into(), prefix: "logs/".into(), enabled: true, expiration_days: 30, abort_multipart_days: 7, noncurrent_expiration_days: 0, ..Default::default() });
    config.quota_bytes = 1 << 30;
    config.object_lock_enabled = true;
    config.default_retention = Some(DefaultRetention { mode: RETENTION_GOVERNANCE, days: 5 });
    config
}

#[test]
fn a_bucket_record_round_trips_with_every_setting() {
    let config = configured();
    let bytes = config.to_bytes().unwrap();
    assert_eq!(BucketConfig::from_bytes(&bytes).unwrap(), config);
    assert_eq!(BucketConfig::from_bytes(&BucketConfig::new(5).to_bytes().unwrap()).unwrap(), BucketConfig::new(5));
}

#[test]
fn a_record_that_breaks_a_rule_is_refused_by_name() {
    let mut no_versioning = configured();
    no_versioning.versioning = VERSIONING_SUSPENDED;
    assert!(no_versioning.to_bytes().is_err(), "object lock needs versioning enabled");
    let mut retention_without_lock = BucketConfig::new(1);
    retention_without_lock.default_retention = Some(DefaultRetention { mode: RETENTION_GOVERNANCE, days: 1 });
    assert!(retention_without_lock.validate().is_err());
    let mut empty_rule = BucketConfig::new(1);
    empty_rule.lifecycle.push(LifecycleRule { id: "x".into(), prefix: String::new(), enabled: true, ..Default::default() });
    assert!(empty_rule.validate().is_err(), "a rule that expires nothing");
    let mut bad_method = BucketConfig::new(1);
    bad_method.cors.push(CorsRule { allowed_origins: vec!["*".into()], allowed_methods: vec!["TRACE".into()], ..Default::default() });
    assert!(bad_method.validate().is_err());
    let mut many_tags = BucketConfig::new(1);
    for index in 0..=MAX_TAGS {
        many_tags.tags.insert(format!("k{index}"), "v".into());
    }
    assert!(many_tags.validate().is_err());
    let mut wrong_version = BucketConfig::new(1);
    wrong_version.format_version = 2;
    assert!(wrong_version.validate().is_err());
    // a record of another version, or damaged, is not read
    let mut bytes = configured().to_bytes().unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    assert!(BucketConfig::from_bytes(&bytes).is_err());
}

#[test]
fn a_record_key_names_its_bucket_and_nothing_else_does() {
    assert_eq!(config_key("photos"), "bucket/photos");
    assert_eq!(bucket_of_key("bucket/photos"), Some("photos"));
    assert_eq!(bucket_of_key("bucket/"), None);
    assert_eq!(bucket_of_key("bucket/a/b"), None);
    assert_eq!(bucket_of_key("other/photos"), None);
}

#[test]
fn xml_elements_are_read_by_name_with_entities_and_without_confusing_a_prefix_name() {
    let body = "<Tagging><TagSet><Tag><Key>a&amp;b</Key><Value>x &lt; y</Value></Tag><Tag><Key>k2</Key><Value/></Tag></TagSet></Tagging>";
    assert_eq!(xml_all(body, "Key"), ["a&b", "k2"]);
    // <TagSet> is not a <Tag>
    assert_eq!(xml_all(body, "Tag").len(), 2);
    assert_eq!(xml_first(body, "Value").as_deref(), Some("x < y"));
    assert_eq!(xml_all(body, "Missing"), Vec::<String>::new());
    assert_eq!(xml_unescape(&xml_escape("<a href=\"x\">'&'</a>")), "<a href=\"x\">'&'</a>");
}

#[test]
fn tagging_cors_lifecycle_versioning_and_lock_documents_round_trip() {
    let config = configured();
    assert_eq!(parse_tagging(&tagging_xml(&config.tags)).unwrap(), config.tags);
    assert_eq!(parse_cors(&cors_xml(&config.cors)).unwrap(), config.cors);
    assert_eq!(parse_lifecycle(&lifecycle_xml(&config.lifecycle)).unwrap(), config.lifecycle);
    assert_eq!(parse_versioning(&versioning_xml(VERSIONING_ENABLED)).unwrap(), VERSIONING_ENABLED);
    assert_eq!(parse_versioning(&versioning_xml(VERSIONING_SUSPENDED)).unwrap(), VERSIONING_SUSPENDED);
    assert!(parse_versioning(&versioning_xml(VERSIONING_NEVER)).is_err(), "there is no 'never' to put");
    let (enabled, retention) = parse_object_lock(&object_lock_xml(&config)).unwrap();
    assert_eq!((enabled, retention), (true, config.default_retention.clone()));
    assert!(parse_tagging("<NotTags/>").is_err());
    assert!(parse_tagging("<Tagging><TagSet><Tag><Key>k</Key></Tag><Tag><Key>k</Key></Tag></TagSet></Tagging>").is_err(), "a repeated key");
    assert!(parse_cors("<CORSConfiguration/>").is_err());
    assert!(parse_lifecycle("<LifecycleConfiguration><Rule><ID>x</ID><Status>Maybe</Status></Rule></LifecycleConfiguration>").is_err());
    assert!(parse_object_lock("<ObjectLockConfiguration><ObjectLockEnabled>Disabled</ObjectLockEnabled></ObjectLockConfiguration>").is_err());
}

#[test]
fn an_allowed_origin_matches_with_one_wildcard() {
    assert!(origin_matches("*", "https://a.example.com"));
    assert!(origin_matches("https://*.example.com", "https://a.example.com"));
    assert!(!origin_matches("https://*.example.com", "https://example.org"));
    assert!(origin_matches("https://a.example.com", "https://a.example.com"));
    assert!(!origin_matches("https://a.example.com", "https://b.example.com"));
    // the head and tail may not overlap
    assert!(!origin_matches("ab*bc", "abc"));
}

#[test]
fn a_lifecycle_rule_keeps_its_filters_counts_and_transition_through_its_document() {
    let rules = vec![
        LifecycleRule {
            id: "full".into(),
            prefix: "logs/".into(),
            enabled: true,
            expiration_days: 90,
            noncurrent_expiration_days: 7,
            abort_multipart_days: 3,
            tag_key: "env".into(),
            tag_value: "tmp".into(),
            noncurrent_versions_to_keep: 2,
            expire_delete_markers: true,
            transition_days: 30,
            transition_storage_class: "GLACIER".into(),
        },
        LifecycleRule { id: "tag-only".into(), enabled: false, expiration_days: 1, tag_key: "a".into(), tag_value: "b".into(), ..Default::default() },
    ];
    let document = lifecycle_xml(&rules);
    assert_eq!(parse_lifecycle(&document).unwrap(), rules, "{document}");
    // two `Days` in one rule are read inside their own elements
    let both = "<LifecycleConfiguration><Rule><Status>Enabled</Status><Expiration><Days>5</Days></Expiration><Transition><Days>2</Days><StorageClass>X</StorageClass></Transition></Rule></LifecycleConfiguration>";
    let parsed = parse_lifecycle(both).unwrap();
    assert_eq!((parsed[0].expiration_days, parsed[0].transition_days), (5, 2));
    // a transition names its days and its class together
    let half = "<LifecycleConfiguration><Rule><Status>Enabled</Status><Transition><Days>2</Days></Transition></Rule></LifecycleConfiguration>";
    assert!(parse_lifecycle(half).is_err());
}

