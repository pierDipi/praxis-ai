// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Praxis Contributors

// Translation of the legacy llm-d coordinator YAML shape into native Praxis
// configuration. This is an input compatibility layer, not runtime or metric
// compatibility with the Go coordinator.

use std::{collections::BTreeSet, time::Duration};

use praxis_core::{config::Config, errors::ProxyError};
use serde::Deserialize;
use serde_yaml::{Mapping, Value};

const MIB: usize = 1024 * 1024;
const DEFAULT_MAX_BODY_MIB: usize = 64;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CoordinatorConfig {
    #[serde(default, rename = "log_level")]
    _log_level: Option<u8>,
    server: CoordinatorServer,
    gateway: CoordinatorGateway,
    pipeline: CoordinatorPipeline,
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct CoordinatorServer {
    listen_addr: String,
    metrics_port: Option<i32>,
    read_timeout: String,
    write_timeout: String,
    shutdown_timeout: String,
    max_request_body_size: usize,
}

impl Default for CoordinatorServer {
    fn default() -> Self {
        Self {
            listen_addr: ":8080".to_owned(),
            metrics_port: Some(9090),
            read_timeout: "30s".to_owned(),
            write_timeout: "120s".to_owned(),
            shutdown_timeout: "25s".to_owned(),
            max_request_body_size: DEFAULT_MAX_BODY_MIB,
        }
    }
}

#[derive(Debug, Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct CoordinatorGateway {
    address: String,
    #[serde(default = "default_max_idle_connections")]
    max_idle_conns_per_host: usize,
    #[serde(default = "default_idle_timeout")]
    idle_conn_timeout: String,
    #[serde(default = "default_gateway_timeout")]
    timeout: String,
    #[serde(default)]
    epp_tls: Option<TlsSettings>,
    #[serde(default)]
    worker_tls: Option<TlsSettings>,
}

#[derive(Debug, Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct TlsSettings {
    #[serde(default)]
    ca_file: Option<String>,
    #[serde(default)]
    cert_file: Option<String>,
    #[serde(default)]
    key_file: Option<String>,
    #[serde(default)]
    server_name: Option<String>,
    #[serde(default = "default_true")]
    verify: bool,
}

#[derive(Debug, Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct CoordinatorPipeline {
    #[serde(default = "default_kv_connector")]
    kv_connector: String,
    #[serde(default = "default_ec_connector")]
    ec_connector: String,
    #[serde(default = "default_true")]
    use_openai_format: bool,
    steps: Vec<CoordinatorStep>,
}

#[derive(Debug, Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct CoordinatorStep {
    #[serde(rename = "type")]
    step_type: String,
    #[serde(default)]
    params: Mapping,
}

fn default_true() -> bool {
    true
}

fn default_max_idle_connections() -> usize {
    200
}

fn default_idle_timeout() -> String {
    "90s".to_owned()
}

fn default_gateway_timeout() -> String {
    "60s".to_owned()
}

fn default_kv_connector() -> String {
    "kv-shared-storage".to_owned()
}

fn default_ec_connector() -> String {
    "ec-shared-storage".to_owned()
}

/// Parse either native Praxis YAML or coordinator-compatible YAML.
pub(crate) fn parse_config_yaml(input: &str) -> Result<Config, ProxyError> {
    let root: Value =
        serde_yaml::from_str(input).map_err(|error| ProxyError::Config(format!("invalid YAML: {error}")))?;
    let mapping = root
        .as_mapping()
        .ok_or_else(|| ProxyError::Config("configuration root must be a mapping".to_owned()))?;

    let keys = mapping.keys().filter_map(Value::as_str).collect::<BTreeSet<_>>();
    let coordinator = ["server", "gateway", "pipeline"].iter().any(|key| keys.contains(key));
    let native = [
        "admin",
        "body_limits",
        "clusters",
        "filter_chains",
        "insecure_options",
        "listeners",
        "metrics",
        "runtime",
        "shutdown_timeout_secs",
        "telemetry",
    ]
    .iter()
    .any(|key| keys.contains(key));

    match (coordinator, native) {
        (true, true) => Err(ProxyError::Config(
            "cannot mix coordinator server/gateway/pipeline keys with native Praxis keys".to_owned(),
        )),
        (true, false) => translate_coordinator(root),
        _ => Config::from_yaml(input),
    }
}

fn translate_coordinator(root: Value) -> Result<Config, ProxyError> {
    let mut source: CoordinatorConfig = serde_yaml::from_value(root)
        .map_err(|error| ProxyError::Config(format!("invalid coordinator configuration: {error}")))?;
    apply_environment_overrides(&mut source)?;
    validate_coordinator(&source)?;

    let read_timeout_ms = duration_ms("server.read_timeout", &source.server.read_timeout)?;
    let write_timeout_ms = duration_ms("server.write_timeout", &source.server.write_timeout)?;
    let shutdown_timeout_secs = duration_secs("server.shutdown_timeout", &source.server.shutdown_timeout)?;
    let max_request_bytes = source
        .server
        .max_request_body_size
        .checked_mul(MIB)
        .ok_or_else(|| ProxyError::Config("server.max_request_body_size overflows bytes".to_owned()))?;
    let address = normalize_listen_address(&source.server.listen_addr)?;

    let topology = infer_topology(&source.pipeline)?;
    let mut prepare_config = Mapping::from_iter([
        (Value::from("topology"), Value::from(topology)),
        (
            Value::from("use_openai_format"),
            Value::from(source.pipeline.use_openai_format),
        ),
        (
            Value::from("kv_connector"),
            Value::from(source.pipeline.kv_connector.clone()),
        ),
        (
            Value::from("ec_connector"),
            Value::from(source.pipeline.ec_connector.clone()),
        ),
        (Value::from("max_body_bytes"), Value::from(max_request_bytes)),
    ]);
    if let Some(step) = source.pipeline.steps.iter().find(|step| step.step_type == "render") {
        if let Some(address) = step.params.get(Value::from("address")).and_then(Value::as_str) {
            prepare_config.insert(Value::from("render_url"), Value::from(address));
        }
        if let Some(timeout) = step.params.get(Value::from("timeout")).and_then(Value::as_str) {
            prepare_config.insert(
                Value::from("render_timeout_ms"),
                Value::from(duration_ms("pipeline.render.timeout", timeout)?),
            );
        }
        for key in ["max_total_tokens", "max_total_placeholder_tokens"] {
            if let Some(value) = step.params.get(Value::from(key)).and_then(Value::as_u64) {
                prepare_config.insert(Value::from(key), Value::from(value));
            }
        }
    }
    if let Some(step) = source
        .pipeline
        .steps
        .iter()
        .find(|step| step.step_type == "replace-media-urls")
    {
        if let Some(timeout) = step.params.get(Value::from("download_timeout")).and_then(Value::as_str) {
            prepare_config.insert(
                Value::from("download_timeout_ms"),
                Value::from(duration_ms("pipeline.replace-media-urls.download_timeout", timeout)?),
            );
        }
        for (source, target) in [
            ("max_concurrent_downloads", "max_concurrent_downloads"),
            ("max_multimodal_entries", "max_multimodal_entries"),
            ("max_download_size", "max_download_bytes"),
        ] {
            if let Some(value) = step.params.get(Value::from(source)).and_then(Value::as_u64) {
                prepare_config.insert(Value::from(target), Value::from(value));
            }
        }
        if let Some(value) = step
            .params
            .get(Value::from("allow_private_networks"))
            .and_then(Value::as_bool)
        {
            prepare_config.insert(Value::from("allow_private_networks"), Value::from(value));
        }
        if let Some(value) = step.params.get(Value::from("allowed_domains")) {
            prepare_config.insert(Value::from("allowed_domains"), value.clone());
        }
    }

    let mut prepare_filter = Mapping::new();
    prepare_filter.insert(Value::from("filter"), Value::from("llmd_prepare"));
    prepare_filter.extend(prepare_config);
    let irr_filter = build_irr_filter(&source.pipeline, &source.gateway, write_timeout_ms, max_request_bytes)?;

    let mut native_root = Mapping::from_iter([
        (
            Value::from("body_limits"),
            Value::Mapping(Mapping::from_iter([
                (Value::from("max_request_bytes"), Value::from(max_request_bytes)),
                (Value::from("max_response_bytes"), Value::from(max_request_bytes)),
            ])),
        ),
        (
            Value::from("listeners"),
            Value::Sequence(vec![Value::Mapping(Mapping::from_iter([
                (Value::from("name"), Value::from("llmd-coordinator")),
                (Value::from("address"), Value::from(address)),
                (Value::from("downstream_read_timeout_ms"), Value::from(read_timeout_ms)),
                (
                    Value::from("filter_chains"),
                    Value::Sequence(vec![Value::from("llmd-coordinator")]),
                ),
            ]))]),
        ),
        (
            Value::from("filter_chains"),
            Value::Sequence(vec![Value::Mapping(Mapping::from_iter([
                (Value::from("name"), Value::from("llmd-coordinator")),
                (
                    Value::from("filters"),
                    Value::Sequence(vec![Value::Mapping(prepare_filter), Value::Mapping(irr_filter)]),
                ),
            ]))]),
        ),
        (Value::from("shutdown_timeout_secs"), Value::from(shutdown_timeout_secs)),
    ]);
    if let Some(port) = source.server.metrics_port.filter(|port| *port > 0) {
        if port > i32::from(u16::MAX) {
            return Err(ProxyError::Config(
                "server.metrics_port must be a valid TCP port or non-positive to disable".to_owned(),
            ));
        }
        native_root.insert(
            Value::from("admin"),
            Value::Mapping(Mapping::from_iter([(
                Value::from("address"),
                Value::from(format!("127.0.0.1:{port}")),
            )])),
        );
    }
    let native = serde_yaml::to_string(&Value::Mapping(native_root))
        .map_err(|error| ProxyError::Config(format!("failed to translate coordinator configuration: {error}")))?;

    Config::from_yaml(&native)
}

fn apply_environment_overrides(config: &mut CoordinatorConfig) -> Result<(), ProxyError> {
    if let Some(value) = environment_value("COORDINATOR_PIPELINE_USE_OPENAI_FORMAT")? {
        config.pipeline.use_openai_format = value.parse::<bool>().map_err(|_| {
            ProxyError::Config("COORDINATOR_PIPELINE_USE_OPENAI_FORMAT must be 'true' or 'false'".to_owned())
        })?;
    }
    if let Some(value) = environment_value("COORDINATOR_SERVER_MAX_REQUEST_BODY_SIZE")? {
        config.server.max_request_body_size = value.parse::<usize>().map_err(|_| {
            ProxyError::Config("COORDINATOR_SERVER_MAX_REQUEST_BODY_SIZE must be an integer number of MiB".to_owned())
        })?;
    }
    if let Some(value) = environment_value("COORDINATOR_SERVER_METRICS_PORT")? {
        config.server.metrics_port = Some(
            value
                .parse::<i32>()
                .map_err(|_| ProxyError::Config("COORDINATOR_SERVER_METRICS_PORT must be an integer".to_owned()))?,
        );
    }
    Ok(())
}

fn environment_value(name: &str) -> Result<Option<String>, ProxyError> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(ProxyError::Config(format!(
            "environment variable {name} is not valid UTF-8"
        ))),
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "the generated IRR shape is clearer when built in stage order"
)]
fn build_irr_filter(
    pipeline: &CoordinatorPipeline,
    gateway: &CoordinatorGateway,
    write_timeout_ms: u64,
    max_body_bytes: usize,
) -> Result<Mapping, ProxyError> {
    let mut stage_specs = Vec::new();
    for step in &pipeline.steps {
        let phase = match step.step_type.as_str() {
            "conditional-decode" => "conditional_decode",
            "encode" => "encode",
            "prefill" => "prefill",
            "decode" => "decode",
            _ => continue,
        };
        stage_specs.push((phase, step));
    }

    let gateway_timeout_ms = duration_ms("gateway.timeout", &gateway.timeout)?;
    let mut steps = Vec::with_capacity(stage_specs.len());
    for (index, (phase, source_step)) in stage_specs.iter().enumerate() {
        let name = format!("{phase}-{index}");
        let next = stage_specs
            .get(index + 1)
            .map(|(next, _)| format!("{next}-{}", index + 1));

        let mut stage_filter = Mapping::from_iter([
            (Value::from("filter"), Value::from("llmd_stage")),
            (Value::from("phase"), Value::from(*phase)),
            (Value::from("max_body_bytes"), Value::from(max_body_bytes)),
        ]);
        for key in ["use_openai_format", "kv_connector", "ec_connector"] {
            if let Some(value) = source_step.params.get(Value::from(key)) {
                stage_filter.insert(Value::from(key), value.clone());
            }
        }
        let ext_proc = Mapping::from_iter([
            (Value::from("filter"), Value::from("ext_proc")),
            (Value::from("target"), Value::from(gateway.address.clone())),
            (Value::from("message_timeout_ms"), Value::from(gateway_timeout_ms)),
            (
                Value::from("lifecycle_timeout_ms"),
                Value::from(write_timeout_ms.max(gateway_timeout_ms)),
            ),
            (Value::from("status_on_error"), Value::from(503)),
            (
                Value::from("processing_mode"),
                Value::Mapping(Mapping::from_iter([
                    (Value::from("request_body_mode"), Value::from("full_duplex_streamed")),
                    (Value::from("response_header_mode"), Value::from("skip")),
                ])),
            ),
        ]);
        let endpoint_selector = Mapping::from_iter([
            (Value::from("filter"), Value::from("endpoint_selector")),
            (
                Value::from("source_header"),
                Value::from("x-gateway-destination-endpoint"),
            ),
            (Value::from("required"), Value::from(true)),
            (Value::from("status_on_required_failure"), Value::from(503)),
            (Value::from("strip_header"), Value::from(true)),
        ]);

        let mut transitions = Vec::new();
        if *phase == "conditional_decode" {
            if let Some(next) = &next {
                transitions.push(Value::Mapping(Mapping::from_iter([
                    (Value::from("status"), Value::Sequence(vec![Value::from(412)])),
                    (Value::from("next"), Value::from(next.clone())),
                ])));
            }
            transitions.push(Value::Mapping(Mapping::from_iter([
                (Value::from("default"), Value::from(true)),
                (Value::from("done"), Value::from(true)),
            ])));
        } else if let Some(next) = next {
            transitions.push(Value::Mapping(Mapping::from_iter([
                (Value::from("default"), Value::from(true)),
                (Value::from("next"), Value::from(next)),
            ])));
        } else {
            transitions.push(Value::Mapping(Mapping::from_iter([
                (Value::from("default"), Value::from(true)),
                (Value::from("done"), Value::from(true)),
            ])));
        }

        let mut irr_step = Mapping::from_iter([
            (Value::from("name"), Value::from(name)),
            (
                Value::from("filters"),
                Value::Sequence(vec![
                    Value::Mapping(stage_filter),
                    Value::Mapping(ext_proc),
                    Value::Mapping(endpoint_selector),
                ]),
            ),
            (Value::from("on_result"), Value::Sequence(transitions)),
        ]);
        if *phase == "encode" {
            let max_concurrency = source_step
                .params
                .get(Value::from("max_parallel"))
                .and_then(Value::as_u64)
                .unwrap_or(8);
            irr_step.insert(
                Value::from("fan_out"),
                Value::Mapping(Mapping::from_iter([
                    (Value::from("max_concurrency"), Value::from(max_concurrency)),
                    (Value::from("max_requests"), Value::from(128)),
                ])),
            );
        }
        steps.push(Value::Mapping(irr_step));
    }

    let initial_step = stage_specs
        .first()
        .map(|(phase, _)| format!("{phase}-0"))
        .ok_or_else(|| ProxyError::Config("pipeline has no orchestration stage".to_owned()))?;
    Ok(Mapping::from_iter([
        (Value::from("filter"), Value::from("iterative_request_router")),
        (Value::from("initial_step"), Value::from(initial_step)),
        (Value::from("max_iterations"), Value::from(stage_specs.len() as u64)),
        (Value::from("timeout_ms"), Value::from(write_timeout_ms)),
        (Value::from("step_timeout_ms"), Value::from(gateway_timeout_ms)),
        (Value::from("max_response_bytes"), Value::from(max_body_bytes)),
        (Value::from("steps"), Value::Sequence(steps)),
    ]))
}

fn infer_topology(pipeline: &CoordinatorPipeline) -> Result<&'static str, ProxyError> {
    let stages = pipeline
        .steps
        .iter()
        .filter_map(|step| match step.step_type.as_str() {
            "encode" | "prefill" | "decode" => Some(step.step_type.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    match stages.as_slice() {
        ["decode"] => Ok("epd"),
        ["prefill", "decode"] => Ok("p-d"),
        ["encode", "decode"] => Ok("e-pd"),
        ["encode", "prefill", "decode"] => Ok("e-p-d"),
        _ => Err(ProxyError::Config(format!(
            "pipeline step sequence {stages:?} does not describe EPD, P/D, E/PD, or E/P/D"
        ))),
    }
}

fn validate_coordinator(config: &CoordinatorConfig) -> Result<(), ProxyError> {
    if config.gateway.address.trim().is_empty() {
        return Err(ProxyError::Config("gateway.address must not be empty".to_owned()));
    }
    for (name, value) in [
        ("gateway.timeout", &config.gateway.timeout),
        ("gateway.idle_conn_timeout", &config.gateway.idle_conn_timeout),
    ] {
        duration_ms(name, value)?;
    }
    if config.gateway.max_idle_conns_per_host == 0 {
        return Err(ProxyError::Config(
            "gateway.max_idle_conns_per_host must be greater than zero".to_owned(),
        ));
    }
    if config.server.max_request_body_size == 0 || config.server.max_request_body_size > 64 {
        return Err(ProxyError::Config(
            "server.max_request_body_size must be between 1 and 64 MiB".to_owned(),
        ));
    }
    validate_tls("gateway.epp_tls", config.gateway.epp_tls.as_ref())?;
    validate_tls("gateway.worker_tls", config.gateway.worker_tls.as_ref())?;
    validate_pipeline(&config.pipeline)
}

fn validate_tls(path: &str, tls: Option<&TlsSettings>) -> Result<(), ProxyError> {
    let Some(tls) = tls else {
        return Ok(());
    };
    let configured = tls.ca_file.is_some()
        || tls.cert_file.is_some()
        || tls.key_file.is_some()
        || tls.server_name.is_some()
        || !tls.verify;
    if configured {
        return Err(ProxyError::Config(format!(
            "{path} is unsupported by the local PoC transport"
        )));
    }
    Ok(())
}

fn validate_pipeline(pipeline: &CoordinatorPipeline) -> Result<(), ProxyError> {
    const KV: [&str; 2] = ["kv-nixl", "kv-shared-storage"];
    const EC: [&str; 2] = ["ec-nixl", "ec-shared-storage"];
    if !KV.contains(&pipeline.kv_connector.as_str()) {
        return Err(ProxyError::Config(format!(
            "pipeline.kv_connector '{}' is unsupported; expected kv-nixl or kv-shared-storage",
            pipeline.kv_connector
        )));
    }
    if !EC.contains(&pipeline.ec_connector.as_str()) {
        return Err(ProxyError::Config(format!(
            "pipeline.ec_connector '{}' is unsupported; expected ec-nixl or ec-shared-storage",
            pipeline.ec_connector
        )));
    }
    if pipeline.steps.is_empty() {
        return Err(ProxyError::Config("pipeline.steps must not be empty".to_owned()));
    }

    let known = [
        "replace-media-urls",
        "render",
        "conditional-decode",
        "encode",
        "prefill",
        "decode",
    ];
    for (index, step) in pipeline.steps.iter().enumerate() {
        if !known.contains(&step.step_type.as_str()) {
            return Err(ProxyError::Config(format!(
                "pipeline.steps[{index}].type '{}' is unsupported",
                step.step_type
            )));
        }
        validate_step_params(index, step)?;
    }

    let has_render = pipeline.steps.iter().any(|step| step.step_type == "render");
    if !pipeline.use_openai_format && !has_render {
        return Err(ProxyError::Config(
            "pipeline.use_openai_format=false requires a render step".to_owned(),
        ));
    }
    let stages = pipeline
        .steps
        .iter()
        .filter_map(|step| match step.step_type.as_str() {
            "conditional-decode" | "encode" | "prefill" | "decode" => Some(step.step_type.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let without_conditional = stages
        .iter()
        .copied()
        .filter(|stage| *stage != "conditional-decode")
        .collect::<Vec<_>>();
    let valid = matches!(
        without_conditional.as_slice(),
        ["decode"] | ["prefill", "decode"] | ["encode", "decode"] | ["encode", "prefill", "decode"]
    );
    let conditional_positions = stages
        .iter()
        .enumerate()
        .filter_map(|(index, stage)| (*stage == "conditional-decode").then_some(index))
        .collect::<Vec<_>>();
    let conditional_valid = conditional_positions.is_empty() || conditional_positions.as_slice() == [0_usize];
    if !valid || !conditional_valid || stages.last() != Some(&"decode") {
        return Err(ProxyError::Config(format!(
            "pipeline step sequence {:?} does not describe EPD, P/D, E/PD, or E/P/D",
            stages
        )));
    }
    Ok(())
}

fn validate_step_params(index: usize, step: &CoordinatorStep) -> Result<(), ProxyError> {
    let allowed: &[&str] = match step.step_type.as_str() {
        "replace-media-urls" => &[
            "download_timeout",
            "max_concurrent_downloads",
            "max_multimodal_entries",
            "max_download_size",
            "allow_private_networks",
            "allowed_domains",
        ],
        "render" => &[
            "address",
            "timeout",
            "max_idle_conns_per_host",
            "idle_conn_timeout",
            "max_total_tokens",
            "max_total_placeholder_tokens",
        ],
        "conditional-decode" => &["use_openai_format"],
        "encode" => &["max_parallel", "use_openai_format", "ec_connector"],
        "prefill" => &["use_openai_format", "ec_connector", "kv_connector"],
        "decode" => &["use_openai_format", "kv_connector"],
        _ => &[],
    };
    for key in step.params.keys().filter_map(Value::as_str) {
        if !allowed.contains(&key) {
            return Err(ProxyError::Config(format!(
                "pipeline.steps[{index}].params.{key} is unsupported"
            )));
        }
    }
    for connector in ["ec_connector", "kv_connector"] {
        if let Some(value) = step.params.get(Value::from(connector)).and_then(Value::as_str) {
            if value.contains("sglang") {
                return Err(ProxyError::Config(format!(
                    "pipeline.steps[{index}].params.{connector} '{value}' is unsupported"
                )));
            }
        }
    }
    Ok(())
}

fn normalize_listen_address(value: &str) -> Result<String, ProxyError> {
    let trimmed = value.trim();
    if let Some(port) = trimmed.strip_prefix(':') {
        port.parse::<u16>()
            .map_err(|_| ProxyError::Config(format!("server.listen_addr has invalid port: {trimmed}")))?;
        return Ok(format!("0.0.0.0:{port}"));
    }
    if trimmed.is_empty() {
        return Err(ProxyError::Config("server.listen_addr must not be empty".to_owned()));
    }
    Ok(trimmed.to_owned())
}

fn duration_ms(path: &str, value: &str) -> Result<u64, ProxyError> {
    let duration = parse_duration(path, value)?;
    u64::try_from(duration.as_millis())
        .map_err(|_| ProxyError::Config(format!("{path} exceeds supported duration")))
        .and_then(|millis| {
            if millis == 0 {
                Err(ProxyError::Config(format!("{path} must be greater than zero")))
            } else {
                Ok(millis)
            }
        })
}

fn duration_secs(path: &str, value: &str) -> Result<u64, ProxyError> {
    let duration = parse_duration(path, value)?;
    let seconds = duration.as_secs();
    if seconds == 0 {
        Err(ProxyError::Config(format!("{path} must be at least one second")))
    } else {
        Ok(seconds)
    }
}

fn parse_duration(path: &str, value: &str) -> Result<Duration, ProxyError> {
    let value = value.trim();
    let (number, multiplier) = if let Some(number) = value.strip_suffix("ms") {
        (number, 1_u64)
    } else if let Some(number) = value.strip_suffix('s') {
        (number, 1_000)
    } else if let Some(number) = value.strip_suffix('m') {
        (number, 60_000)
    } else if let Some(number) = value.strip_suffix('h') {
        (number, 3_600_000)
    } else {
        return Err(ProxyError::Config(format!("{path} must use an ms, s, m, or h suffix")));
    };
    let amount = number
        .parse::<u64>()
        .map_err(|_| ProxyError::Config(format!("{path} has invalid duration '{value}'")))?;
    let millis = amount
        .checked_mul(multiplier)
        .ok_or_else(|| ProxyError::Config(format!("{path} exceeds supported duration")))?;
    Ok(Duration::from_millis(millis))
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = r#"
server:
  listen_addr: ":8181"
  read_timeout: 10s
  write_timeout: 2m
  shutdown_timeout: 25s
  max_request_body_size: 16
gateway:
  address: http://epp:9002
pipeline:
  use_openai_format: true
  steps:
    - type: render
      params: { address: http://render:8080 }
    - type: encode
      params: { max_parallel: 4 }
    - type: prefill
    - type: decode
"#;

    #[test]
    fn translates_coordinator_config() {
        let config = parse_config_yaml(BASE).unwrap();
        assert_eq!(config.listeners[0].address, "0.0.0.0:8181");
        assert_eq!(config.listeners[0].downstream_read_timeout_ms, Some(10_000));
        assert_eq!(config.body_limits.max_request_bytes, Some(16 * MIB));
        assert_eq!(config.shutdown_timeout_secs, 25);
        assert_eq!(config.admin.address.as_deref(), Some("127.0.0.1:9090"));
        let filter = &config.filter_chains[0].filters[0];
        assert_eq!(filter.filter_type, "llmd_prepare");
        assert_eq!(filter.config["topology"].as_str(), Some("e-p-d"));
        assert_eq!(filter.config["use_openai_format"].as_bool(), Some(true));
        assert_eq!(filter.config["max_body_bytes"].as_u64(), Some((16 * MIB) as u64));
        assert_eq!(filter.config["render_url"].as_str(), Some("http://render:8080"));
        let irr = &config.filter_chains[0].filters[1];
        assert_eq!(irr.filter_type, "iterative_request_router");
        assert_eq!(irr.config["initial_step"].as_str(), Some("encode-0"));
        let steps = irr.config["steps"].as_sequence().unwrap();
        assert_eq!(steps.len(), 3);
        assert_eq!(steps[0]["fan_out"]["max_concurrency"].as_u64(), Some(4));
        assert_eq!(steps[0]["filters"][0]["filter"].as_str(), Some("llmd_stage"));
        assert_eq!(steps[0]["filters"][1]["target"].as_str(), Some("http://epp:9002"));
    }

    #[test]
    fn leaves_native_config_unchanged() {
        let config = parse_config_yaml(
            "listeners: [{ name: web, address: '127.0.0.1:8080', filter_chains: [main] }]\n\
             filter_chains: [{ name: main, filters: [] }]",
        )
        .unwrap();
        assert_eq!(config.listeners[0].name, "web");
    }

    #[test]
    fn rejects_mixed_schema() {
        let error = parse_config_yaml("server: {}\ngateway: { address: x }\npipeline: { steps: [] }\nlisteners: []")
            .unwrap_err();
        assert!(error.to_string().contains("cannot mix"));
    }

    #[test]
    fn rejects_sglang_and_unknown_fields() {
        let sglang = BASE.replace("pipeline:\n", "pipeline:\n  kv_connector: kv-sglang\n");
        assert!(
            parse_config_yaml(&sglang)
                .unwrap_err()
                .to_string()
                .contains("unsupported")
        );
        let unknown = BASE.replace("  listen_addr", "  mystery: true\n  listen_addr");
        assert!(
            parse_config_yaml(&unknown)
                .unwrap_err()
                .to_string()
                .contains("unknown field")
        );
    }

    #[test]
    fn tokens_in_requires_render() {
        let yaml = r#"
server: {}
gateway: { address: http://epp:9002 }
pipeline:
  use_openai_format: false
  steps: [{ type: prefill }, { type: decode }]
"#;
        assert!(
            parse_config_yaml(yaml)
                .unwrap_err()
                .to_string()
                .contains("requires a render")
        );
    }

    #[test]
    fn accepts_all_four_topologies() {
        for steps in [
            "    - type: decode\n",
            "    - type: prefill\n    - type: decode\n",
            "    - type: encode\n    - type: decode\n",
            "    - type: encode\n    - type: prefill\n    - type: decode\n",
        ] {
            let yaml = format!("server: {{}}\ngateway: {{ address: 'http://epp:9002' }}\npipeline:\n  steps:\n{steps}");
            parse_config_yaml(&yaml).unwrap_or_else(|error| panic!("topology should parse: {error}"));
        }
    }

    #[test]
    fn validates_tls_pairs() {
        let yaml = BASE.replace(
            "  address: http://epp:9002",
            "  address: https://epp:9002\n  epp_tls: { cert_file: client.pem }",
        );
        let error = parse_config_yaml(&yaml).unwrap_err().to_string();
        assert!(error.contains("unsupported by the local PoC transport"));
    }

    #[test]
    fn preserves_step_level_overrides() {
        let yaml = BASE.replace(
            "    - type: prefill\n",
            concat!(
                "    - type: prefill\n",
                "      params:\n",
                "        use_openai_format: false\n",
                "        kv_connector: kv-nixl\n",
                "        ec_connector: ec-nixl\n",
            ),
        );
        let config = parse_config_yaml(&yaml).unwrap();
        let steps = config.filter_chains[0].filters[1].config["steps"]
            .as_sequence()
            .unwrap();
        let stage = &steps[1]["filters"][0];
        assert_eq!(stage["use_openai_format"].as_bool(), Some(false));
        assert_eq!(stage["kv_connector"].as_str(), Some("kv-nixl"));
        assert_eq!(stage["ec_connector"].as_str(), Some("ec-nixl"));
    }

    #[test]
    fn conditional_decode_routes_only_412_to_next_stage() {
        let yaml = BASE.replace(
            "    - type: encode\n",
            "    - type: conditional-decode\n    - type: encode\n",
        );
        let config = parse_config_yaml(&yaml).unwrap();
        let steps = config.filter_chains[0].filters[1].config["steps"]
            .as_sequence()
            .unwrap();
        assert_eq!(steps[0]["on_result"][0]["status"][0].as_u64(), Some(412));
        assert_eq!(steps[0]["on_result"][0]["next"].as_str(), Some("encode-1"));
        assert_eq!(steps[0]["on_result"][1]["done"].as_bool(), Some(true));
    }
}
