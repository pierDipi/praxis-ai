// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Praxis Contributors

//! Filters that initialize and advance llm-d inference stages.

use async_trait::async_trait;
use bytes::Bytes;
use praxis_filter::{
    BodyAccess, BodyMode, FanOutRequest, FanOutRequests, FanOutResponses, FilterAction, FilterError, HttpFilter,
    HttpFilterContext, Rejection, SubRequest, SubRequestResponseMode, parse_filter_config,
};
use serde::Deserialize;
use serde_json::{Map, Value};

use super::{
    EcConnector, KvConnector,
    preprocessing::{self, MediaConfig},
    state::{CoordinatorState, PreprocessingLimits, Topology, WireFormat},
    transform,
};

const MAX_BODY_BYTES: usize = 67_108_864;
const fn default_true() -> bool {
    true
}
const fn default_max_body_bytes() -> usize {
    MAX_BODY_BYTES
}
const fn default_download_timeout_ms() -> u64 {
    10_000
}
const fn default_render_timeout_ms() -> u64 {
    30_000
}
const fn default_max_media_entries() -> usize {
    16
}
const fn default_max_concurrent_downloads() -> usize {
    8
}
const fn default_max_download_bytes() -> usize {
    10 * 1024 * 1024
}
const fn default_max_tokens() -> usize {
    131_072
}
const fn default_max_placeholder_tokens() -> usize {
    65_536
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PrepareConfig {
    topology: Topology,
    #[serde(default = "default_true")]
    use_openai_format: bool,
    #[serde(default)]
    kv_connector: KvConnector,
    #[serde(default)]
    ec_connector: EcConnector,
    #[serde(default = "default_max_body_bytes")]
    max_body_bytes: usize,
    #[serde(default)]
    render_url: Option<String>,
    #[serde(default = "default_download_timeout_ms")]
    download_timeout_ms: u64,
    #[serde(default = "default_render_timeout_ms")]
    render_timeout_ms: u64,
    #[serde(default = "default_max_media_entries")]
    max_multimodal_entries: usize,
    #[serde(default = "default_max_concurrent_downloads")]
    max_concurrent_downloads: usize,
    #[serde(default = "default_max_download_bytes")]
    max_download_bytes: usize,
    #[serde(default)]
    allow_private_networks: bool,
    #[serde(default)]
    allowed_domains: Vec<String>,
    #[serde(default = "default_max_tokens")]
    max_total_tokens: usize,
    #[serde(default = "default_max_placeholder_tokens")]
    max_total_placeholder_tokens: usize,
}

/// Parse the admitted request and initialize coordinator state.
pub struct LlmdPrepareFilter {
    config: PrepareConfig,
    render_client: reqwest::Client,
}

impl LlmdPrepareFilter {
    /// Construct from YAML configuration.
    pub fn from_config(value: &serde_yaml::Value) -> Result<Box<dyn HttpFilter>, FilterError> {
        let config: PrepareConfig = parse_filter_config("llmd_prepare", value)?;
        if config.max_body_bytes == 0 || config.max_body_bytes > MAX_BODY_BYTES {
            return Err(format!("llmd_prepare: max_body_bytes must be 1..={MAX_BODY_BYTES}").into());
        }
        if config.max_multimodal_entries == 0
            || config.max_concurrent_downloads == 0
            || config.max_download_bytes == 0
            || config.max_total_tokens == 0
            || config.max_total_placeholder_tokens == 0
        {
            return Err("llmd_prepare: preprocessing limits must be positive".into());
        }
        let render_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(config.render_timeout_ms))
            .build()
            .map_err(|error| FilterError::from(format!("llmd_prepare: render client: {error}")))?;
        Ok(Box::new(Self { config, render_client }))
    }
}

#[async_trait]
impl HttpFilter for LlmdPrepareFilter {
    fn name(&self) -> &'static str {
        "llmd_prepare"
    }
    fn request_body_access(&self) -> BodyAccess {
        BodyAccess::ReadOnly
    }
    fn request_body_mode(&self) -> BodyMode {
        BodyMode::StreamBuffer {
            max_bytes: Some(self.config.max_body_bytes),
        }
    }

    async fn on_request(&self, ctx: &mut HttpFilterContext<'_>) -> Result<FilterAction, FilterError> {
        let bytes = ctx
            .buffered_request_body
            .as_ref()
            .ok_or_else(|| FilterError::from("llmd_prepare: buffered request body unavailable"))?;
        let mut body: Map<String, Value> = match serde_json::from_slice(bytes) {
            Ok(body) => body,
            Err(_) => {
                return Ok(FilterAction::Reject(
                    Rejection::status(400).with_body("invalid JSON request body"),
                ));
            },
        };
        let path = ctx.request.uri.path();
        let input_format = WireFormat::detect(path, true);
        let format = WireFormat::detect(path, self.config.use_openai_format);
        let media = MediaConfig {
            timeout: std::time::Duration::from_millis(self.config.download_timeout_ms),
            max_concurrent_downloads: self.config.max_concurrent_downloads,
            max_entries: self.config.max_multimodal_entries,
            max_bytes: self.config.max_download_bytes,
            allow_private_networks: self.config.allow_private_networks,
            allowed_domains: self.config.allowed_domains.clone(),
        };
        let media_count = match preprocessing::normalize_media(&mut body, &media).await {
            Ok(count) => count,
            Err(error) => {
                let status = if error.client { 400 } else { 502 };
                return Ok(FilterAction::Reject(Rejection::status(status).with_body(error.message)));
            },
        };
        let limits = PreprocessingLimits {
            max_total_tokens: self.config.max_total_tokens,
            max_total_placeholder_tokens: self.config.max_total_placeholder_tokens,
        };
        let rendered = if input_format == WireFormat::Completions && body.get("prompt").is_some_and(Value::is_array) {
            let wrapper = serde_json::json!({"token_ids": body.get("prompt")});
            preprocessing::parse_rendered(&wrapper, None, limits)
        } else if input_format == WireFormat::Generate {
            preprocessing::parse_rendered_map(&body, None, limits)
        } else if let Some(render_url) = self.config.render_url.as_deref() {
            preprocessing::render(
                &self.render_client,
                render_url,
                input_format,
                &body,
                media_count,
                limits,
            )
            .await
        } else {
            Err(preprocessing::PreprocessError {
                client: false,
                message: "render_url is required for OpenAI text inputs".to_owned(),
            })
        };
        let (token_ids, multimodal) = match rendered {
            Ok(rendered) => rendered,
            Err(error) => {
                let status = if error.client { 400 } else { 502 };
                return Ok(FilterAction::Reject(Rejection::status(status).with_body(error.message)));
            },
        };
        let state = CoordinatorState {
            original_path: ctx.request.uri.path().to_owned(),
            body,
            topology: self.config.topology,
            format,
            token_ids,
            multimodal,
            ec_transfer_params: Map::new(),
            kv_transfer_params: Map::new(),
            ec_connector: self.config.ec_connector,
            kv_connector: self.config.kv_connector,
        };
        let requests = if state.needs_encode() {
            transform::encode_bodies(&state)
                .into_iter()
                .map(|(key, body)| {
                    let body = serde_json::to_vec(&body).map(Bytes::from).map_err(|error| {
                        FilterError::from(format!("llmd_prepare: serialize encode request: {error}"))
                    })?;
                    let uri = state
                        .format
                        .path()
                        .parse()
                        .map_err(|error| FilterError::from(format!("llmd_prepare: encode URI: {error}")))?;
                    Ok(FanOutRequest {
                        key,
                        request: SubRequest {
                            method: http::Method::POST,
                            uri,
                            // Fan-out requests execute independently, so each owns a header map.
                            headers: ctx.request.headers.clone(),
                            body,
                        },
                    })
                })
                .collect::<Result<Vec<_>, FilterError>>()?
        } else {
            // Encode remains a configured fan-out step for E/PD and E/P/D,
            // even when this request has no encodable media (including native
            // generate requests). An empty batch advances the IRR transition
            // without contacting an encode worker.
            Vec::new()
        };
        ctx.extensions.insert(FanOutRequests(requests));
        ctx.extensions.insert(state);
        for name in [
            "epp-profile",
            "x-gateway-destination-endpoint",
            "kv-transfer-params",
            "ec-transfer-params",
        ] {
            ctx.request_headers_to_remove.push(
                name.parse()
                    .map_err(|e| FilterError::from(format!("invalid internal header: {e}")))?,
            );
        }
        Ok(FilterAction::Continue)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum Stage {
    ConditionalDecode,
    Encode,
    Prefill,
    Decode,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StageConfig {
    phase: Stage,
    #[serde(default)]
    use_openai_format: Option<bool>,
    #[serde(default)]
    kv_connector: Option<KvConnector>,
    #[serde(default)]
    ec_connector: Option<EcConnector>,
    #[serde(default = "default_max_body_bytes")]
    max_body_bytes: usize,
}

/// Transform one IRR request into an llm-d worker stage.
pub struct LlmdStageFilter {
    config: StageConfig,
}

impl LlmdStageFilter {
    /// Construct from YAML configuration.
    pub fn from_config(value: &serde_yaml::Value) -> Result<Box<dyn HttpFilter>, FilterError> {
        let config: StageConfig = parse_filter_config("llmd_stage", value)?;
        if config.max_body_bytes == 0 || config.max_body_bytes > MAX_BODY_BYTES {
            return Err(format!("llmd_stage: max_body_bytes must be 1..={MAX_BODY_BYTES}").into());
        }
        Ok(Box::new(Self { config }))
    }
    fn profile(&self) -> &'static str {
        match self.config.phase {
            Stage::ConditionalDecode | Stage::Decode => "decode",
            Stage::Encode => "encode",
            Stage::Prefill => "prefill",
        }
    }
}

#[async_trait]
impl HttpFilter for LlmdStageFilter {
    fn name(&self) -> &'static str {
        "llmd_stage"
    }
    fn request_body_access(&self) -> BodyAccess {
        BodyAccess::ReadWrite
    }
    fn response_body_access(&self) -> BodyAccess {
        if self.config.phase == Stage::Prefill {
            BodyAccess::ReadOnly
        } else {
            BodyAccess::None
        }
    }
    fn request_body_mode(&self) -> BodyMode {
        BodyMode::StreamBuffer {
            max_bytes: Some(self.config.max_body_bytes),
        }
    }
    fn response_body_mode(&self) -> BodyMode {
        if self.config.phase == Stage::Prefill {
            BodyMode::StreamBuffer {
                max_bytes: Some(self.config.max_body_bytes),
            }
        } else {
            BodyMode::Stream
        }
    }
    fn may_select_streaming_subrequest_response(&self) -> bool {
        matches!(self.config.phase, Stage::ConditionalDecode | Stage::Decode)
    }

    async fn on_request(&self, ctx: &mut HttpFilterContext<'_>) -> Result<FilterAction, FilterError> {
        if self.config.phase == Stage::Encode {
            ctx.request_headers_to_set.push((
                "epp-profile".parse().expect("static header"),
                "encode".parse().expect("static value"),
            ));
            return Ok(FilterAction::Continue);
        }
        if self.config.phase == Stage::Prefill
            && let Some(responses) = ctx.extensions.remove::<FanOutResponses>()
        {
            let parsed = responses
                .0
                .into_iter()
                .map(|item| {
                    serde_json::from_slice::<Map<String, Value>>(&item.response.body).map_err(|e| {
                        FilterError::from(format!("llmd_stage: invalid encode response '{}': {e}", item.key))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let state = ctx
                .extensions
                .get_mut::<CoordinatorState>()
                .ok_or_else(|| FilterError::from("llmd_stage: coordinator state missing"))?;
            let connector = self.config.ec_connector.unwrap_or(state.ec_connector);
            state.ec_transfer_params = connector
                .merge_encode_responses(parsed)
                .map_err(|e| FilterError::from(format!("llmd_stage: {e}")))?;
        }
        let state = ctx
            .extensions
            .get::<CoordinatorState>()
            .ok_or_else(|| FilterError::from("llmd_stage: coordinator state missing"))?;
        let format = self
            .config
            .use_openai_format
            .map_or(state.format, |openai| WireFormat::detect(&state.original_path, openai));
        let worker_path = format.path();
        if self.config.phase == Stage::ConditionalDecode && !state.multimodal.is_empty() {
            return Ok(FilterAction::Reject(Rejection::status(412)));
        }
        if ctx.request.uri.path() != worker_path {
            ctx.rewritten_path = Some(worker_path.to_owned());
        }
        ctx.request_headers_to_set.push((
            "epp-profile".parse().expect("static header"),
            self.profile().parse().expect("static value"),
        ));
        if self.config.phase == Stage::ConditionalDecode {
            ctx.request_headers_to_set.push((
                "prefer".parse().expect("static header"),
                "if-available".parse().expect("static value"),
            ));
        }
        if matches!(self.config.phase, Stage::ConditionalDecode | Stage::Decode) {
            ctx.set_subrequest_response_mode(SubRequestResponseMode::Streaming);
        }
        Ok(FilterAction::Continue)
    }

    async fn on_request_body(
        &self,
        ctx: &mut HttpFilterContext<'_>,
        body: &mut Option<Bytes>,
        end: bool,
    ) -> Result<FilterAction, FilterError> {
        if !end {
            return Ok(FilterAction::Continue);
        }
        if self.config.phase == Stage::Encode {
            ctx.request_headers_to_set.push((
                "epp-profile".parse().expect("static header"),
                "encode".parse().expect("static value"),
            ));
            return Ok(FilterAction::Continue);
        }
        let state = ctx
            .extensions
            .get::<CoordinatorState>()
            .ok_or_else(|| FilterError::from("llmd_stage: coordinator state missing"))?;
        let format = self
            .config
            .use_openai_format
            .map_or(state.format, |openai| WireFormat::detect(&state.original_path, openai));
        let worker_path = format.path();
        if ctx.request.uri.path() != worker_path {
            ctx.rewritten_path = Some(worker_path.to_owned());
        }
        ctx.request_headers_to_set.push((
            "epp-profile".parse().expect("static header"),
            self.profile().parse().expect("static value"),
        ));
        let kv_connector = self.config.kv_connector.unwrap_or(state.kv_connector);
        let value = match self.config.phase {
            Stage::ConditionalDecode => Ok(transform::conditional_decode_body(state, format)),
            Stage::Decode => transform::decode_body_with(state, format, kv_connector),
            Stage::Prefill => transform::prefill_body_with(state, format, kv_connector),
            Stage::Encode => unreachable!(),
        }
        .map_err(|e| FilterError::from(format!("llmd_stage: {e}")))?;
        *body = Some(Bytes::from(
            serde_json::to_vec(&value).map_err(|e| FilterError::from(format!("llmd_stage: serialize: {e}")))?,
        ));
        Ok(FilterAction::Continue)
    }

    fn on_response_body(
        &self,
        ctx: &mut HttpFilterContext<'_>,
        body: &mut Option<Bytes>,
        end: bool,
    ) -> Result<FilterAction, FilterError> {
        if !end || self.config.phase != Stage::Prefill {
            return Ok(FilterAction::Continue);
        }
        let response: Value = serde_json::from_slice(body.as_deref().unwrap_or_default())
            .map_err(|e| FilterError::from(format!("llmd_stage: invalid prefill response: {e}")))?;
        let params = response
            .get("kv_transfer_params")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        ctx.extensions
            .get_mut::<CoordinatorState>()
            .ok_or_else(|| FilterError::from("llmd_stage: coordinator state missing"))?
            .kv_transfer_params = params;
        Ok(FilterAction::Continue)
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::*;
    #[test]
    fn zero_body_limit_is_invalid() {
        let cfg = serde_yaml::from_str("topology: e-p-d\nmax_body_bytes: 0").unwrap();
        assert!(LlmdPrepareFilter::from_config(&cfg).is_err());
    }
}
