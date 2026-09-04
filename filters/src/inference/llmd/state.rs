// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Praxis Contributors

//! Request-scoped state for llm-d disaggregated inference.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::{EcConnector, KvConnector};

/// Deployment topology driven by the coordinator pipeline.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Topology {
    /// Encode, prefill, and decode execute on one decode worker.
    #[serde(rename = "epd")]
    Combined,
    /// Prefill and decode execute on separate workers.
    #[serde(rename = "p-d")]
    PrefillDecode,
    /// Encode is separate; prefill and decode share a worker.
    #[serde(rename = "e-pd")]
    EncodePrefillDecode,
    /// Encode, prefill, and decode execute on separate workers.
    #[serde(rename = "e-p-d")]
    FullyDisaggregated,
}

/// Worker request encoding.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum WireFormat {
    /// `/v1/chat/completions`.
    #[default]
    ChatCompletions,
    /// `/v1/completions`.
    Completions,
    /// `/inference/v1/generate` tokens-in format.
    Generate,
}

impl WireFormat {
    /// Detect the incoming request format.
    #[must_use]
    pub fn detect(path: &str, use_openai_format: bool) -> Self {
        if path == "/v1/completions" {
            return Self::Completions;
        }
        if !use_openai_format || path == "/inference/v1/generate" {
            return Self::Generate;
        }
        Self::ChatCompletions
    }

    /// Worker request path.
    #[must_use]
    pub const fn path(self) -> &'static str {
        match self {
            Self::ChatCompletions => "/v1/chat/completions",
            Self::Completions => "/v1/completions",
            Self::Generate => "/inference/v1/generate",
        }
    }
}

/// Token range occupied by one multimodal placeholder.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PlaceholderRange {
    /// First placeholder token index.
    pub offset: usize,
    /// Number of placeholder tokens.
    pub length: usize,
}

/// Rendered state for one multimodal input.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MultimodalEntry {
    /// Stable position in the request.
    pub index: usize,
    /// Cache key returned by the renderer.
    pub hash: String,
    /// Optional base64 tensor; empty means resolve it from encoder cache.
    pub kwargs_data: String,
    /// Placeholder range in the rendered token sequence.
    pub placeholder: PlaceholderRange,
}

/// State moved between coordinator stages for one client request.
#[derive(Clone, Debug)]
pub struct CoordinatorState {
    /// Original client API path.
    pub original_path: String,
    /// Mutable provider request object.
    pub body: Map<String, Value>,
    /// Selected topology.
    pub topology: Topology,
    /// Worker wire format.
    pub format: WireFormat,
    /// Token IDs produced by render or supplied by the client.
    pub token_ids: Vec<u64>,
    /// Rendered multimodal entries.
    pub multimodal: Vec<MultimodalEntry>,
    /// Encoder-cache descriptor merged from encode workers.
    pub ec_transfer_params: Map<String, Value>,
    /// KV descriptor returned by prefill.
    pub kv_transfer_params: Map<String, Value>,
    /// Configured EC connector.
    pub ec_connector: EcConnector,
    /// Configured KV connector.
    pub kv_connector: KvConnector,
}

/// Limits applied before worker scheduling begins.
#[derive(Clone, Copy, Debug)]
pub struct PreprocessingLimits {
    /// Maximum number of token IDs accepted from render or generate.
    pub max_total_tokens: usize,
    /// Maximum sum of multimodal placeholder lengths.
    pub max_total_placeholder_tokens: usize,
}

impl CoordinatorState {
    /// Whether this request needs a remote encode stage.
    #[must_use]
    pub fn needs_encode(&self) -> bool {
        !self.multimodal.is_empty()
            && matches!(
                self.topology,
                Topology::EncodePrefillDecode | Topology::FullyDisaggregated
            )
    }
}
