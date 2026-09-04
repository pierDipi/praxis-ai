// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Praxis Contributors

//! llm-d worker request transformations.

use serde_json::{Map, Value, json};

use super::{
    KvConnector,
    state::{CoordinatorState, MultimodalEntry, WireFormat},
};

fn features(entries: &[MultimodalEntry], include_kwargs: bool) -> Option<Value> {
    if entries.is_empty() {
        return None;
    }
    let hashes = entries.iter().map(|entry| entry.hash.clone()).collect::<Vec<_>>();
    let placeholders = entries
        .iter()
        .map(|entry| {
            json!({
                "offset": entry.placeholder.offset,
                "length": entry.placeholder.length
            })
        })
        .collect::<Vec<_>>();
    let mut value = Map::from_iter([
        ("mm_hashes".to_owned(), json!({"image": hashes})),
        ("mm_placeholders".to_owned(), json!({"image": placeholders})),
    ]);
    if include_kwargs {
        let kwargs = entries
            .iter()
            .map(|entry| (!entry.kwargs_data.is_empty()).then(|| Value::String(entry.kwargs_data.clone())))
            .collect::<Vec<_>>();
        value.insert("kwargs_data".to_owned(), json!({"image": kwargs}));
    }
    Some(Value::Object(value))
}

fn cap_single_token(body: &mut Map<String, Value>, format: WireFormat) {
    if format == WireFormat::Generate {
        let target = body
            .entry("sampling_params")
            .or_insert_with(|| Value::Object(Map::new()))
            .as_object_mut()
            .expect("sampling_params inserted as object");
        target.insert("max_tokens".to_owned(), json!(1));
        target.remove("min_tokens");
    } else {
        body.insert("max_tokens".to_owned(), json!(1));
        body.remove("min_tokens");
    }
    if body.contains_key("max_completion_tokens") {
        body.insert("max_completion_tokens".to_owned(), json!(1));
    }
    body.insert("stream".to_owned(), Value::Bool(false));
    body.remove("stream_options");
}

fn set_generate_transfer(body: &mut Map<String, Value>, kv: Value, ec: Option<Value>) -> Result<(), String> {
    let sampling = body
        .entry("sampling_params")
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
        .ok_or_else(|| "sampling_params must be an object".to_owned())?;
    let extra = sampling
        .entry("extra_args")
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
        .ok_or_else(|| "sampling_params.extra_args must be an object".to_owned())?;
    extra.insert("kv_transfer_params".to_owned(), kv);
    if let Some(ec) = ec.filter(|value| value.as_object().is_some_and(|map| !map.is_empty())) {
        extra.insert("ec_transfer_params".to_owned(), ec);
    }
    Ok(())
}

/// Build the bounded single-token prefill request.
#[cfg(test)]
pub fn prefill_body(state: &CoordinatorState) -> Result<Value, String> {
    prefill_body_with(state, state.format, state.kv_connector)
}

/// Build prefill body with step-level wire and connector overrides.
pub fn prefill_body_with(
    state: &CoordinatorState,
    format: WireFormat,
    connector: KvConnector,
) -> Result<Value, String> {
    let kv = connector.prefill_params();
    let ec = Value::Object(state.ec_transfer_params.clone());
    let mut body = match format {
        WireFormat::ChatCompletions => state.body.clone(),
        WireFormat::Completions => {
            let mut body = state.body.clone();
            body.insert("prompt".to_owned(), json!(state.token_ids));
            body
        },
        WireFormat::Generate => Map::from_iter([
            (
                "model".to_owned(),
                state.body.get("model").cloned().unwrap_or(Value::Null),
            ),
            ("token_ids".to_owned(), json!(state.token_ids)),
            ("sampling_params".to_owned(), json!({"max_tokens": 1})),
        ]),
    };
    match format {
        WireFormat::ChatCompletions => {
            body.insert("kv_transfer_params".to_owned(), kv);
            if let Some(features) = features(&state.multimodal, true) {
                body.insert("ec_transfer_params".to_owned(), ec);
                body.insert(
                    "tokens".to_owned(),
                    json!({
                        "token_ids": state.token_ids,
                        "features": features
                    }),
                );
            }
        },
        WireFormat::Completions => {
            body.insert("kv_transfer_params".to_owned(), kv);
            if !state.ec_transfer_params.is_empty() {
                body.insert("ec_transfer_params".to_owned(), ec);
            }
        },
        WireFormat::Generate => {
            if let Some(features) = features(&state.multimodal, true) {
                body.insert("features".to_owned(), features);
            }
            set_generate_transfer(&mut body, kv, Some(ec))?;
        },
    }
    cap_single_token(&mut body, format);
    Ok(Value::Object(body))
}

/// Build the terminal decode request.
#[cfg(test)]
pub fn decode_body(state: &CoordinatorState) -> Result<Value, String> {
    decode_body_with(state, state.format, state.kv_connector)
}

/// Build the speculative decode request without remote-transfer parameters.
pub fn conditional_decode_body(state: &CoordinatorState, format: WireFormat) -> Value {
    let mut body = state.body.clone();
    match format {
        WireFormat::ChatCompletions => {
            if !state.token_ids.is_empty() {
                let mut tokens = Map::from_iter([("token_ids".to_owned(), json!(state.token_ids))]);
                if let Some(features) = features(&state.multimodal, false) {
                    tokens.insert("features".to_owned(), features);
                }
                body.insert("tokens".to_owned(), Value::Object(tokens));
            }
        },
        WireFormat::Completions => {
            if !state.token_ids.is_empty() {
                body.insert("prompt".to_owned(), json!(state.token_ids));
            }
        },
        WireFormat::Generate => {},
    }
    Value::Object(body)
}

/// Build one bounded encoder request per rendered multimodal entry.
pub fn encode_bodies(state: &CoordinatorState) -> Vec<(String, Value)> {
    let image_parts = state
        .body
        .get("messages")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|message| message.get("content").and_then(Value::as_array))
        .flatten()
        .filter(|part| part.get("type").and_then(Value::as_str) == Some("image_url"))
        .cloned()
        .collect::<Vec<_>>();
    state.multimodal.iter().map(|entry| {
        let bos = state.token_ids.first().copied().unwrap_or(1);
        let placeholder = state.token_ids.get(entry.placeholder.offset).copied().unwrap_or(0);
        let mut token_ids = Vec::with_capacity(entry.placeholder.length.saturating_add(1));
        token_ids.push(bos);
        token_ids.resize(entry.placeholder.length.saturating_add(1), placeholder);
        let feature = json!({
            "mm_hashes": {"image": [&entry.hash]},
            "mm_placeholders": {"image": [{"offset": 1, "length": entry.placeholder.length}]},
            "kwargs_data": {"image": [&entry.kwargs_data]},
        });
        let mut body = match state.format {
            WireFormat::ChatCompletions | WireFormat::Completions => json!({
                "model": state.body.get("model").cloned().unwrap_or(Value::Null),
                "messages": [{"role": "user", "content": image_parts.get(entry.index).cloned().into_iter().collect::<Vec<_>>() }],
                "tokens": {"token_ids": token_ids, "features": feature},
            }),
            WireFormat::Generate => json!({
                "model": state.body.get("model").cloned().unwrap_or(Value::Null),
                "token_ids": token_ids,
                "features": feature,
            }),
        };
        cap_single_token(body.as_object_mut().expect("encoder body is an object"), state.format);
        (entry.hash.clone(), body)
    }).collect()
}

/// Build decode body with step-level wire and connector overrides.
pub fn decode_body_with(state: &CoordinatorState, format: WireFormat, connector: KvConnector) -> Result<Value, String> {
    let kv = connector.decode_params(&state.kv_transfer_params);
    let mut body = match format {
        WireFormat::Generate => {
            let mut sampling = Map::new();
            for key in [
                "max_tokens",
                "min_tokens",
                "temperature",
                "top_p",
                "top_k",
                "stop",
                "seed",
            ] {
                if let Some(value) = state.body.get(key) {
                    sampling.insert(key.to_owned(), value.clone());
                }
            }
            let mut body = Map::from_iter([
                (
                    "model".to_owned(),
                    state.body.get("model").cloned().unwrap_or(Value::Null),
                ),
                ("token_ids".to_owned(), json!(state.token_ids)),
                ("sampling_params".to_owned(), Value::Object(sampling)),
            ]);
            if let Some(features) = features(&state.multimodal, false) {
                body.insert("features".to_owned(), features);
            }
            body
        },
        WireFormat::ChatCompletions | WireFormat::Completions => state.body.clone(),
    };
    match format {
        WireFormat::ChatCompletions => {
            body.insert("kv_transfer_params".to_owned(), kv);
            let mut tokens = Map::from_iter([("token_ids".to_owned(), json!(state.token_ids))]);
            if let Some(features) = features(&state.multimodal, false) {
                tokens.insert("features".to_owned(), features);
            }
            body.insert("tokens".to_owned(), Value::Object(tokens));
        },
        WireFormat::Completions => {
            body.insert("kv_transfer_params".to_owned(), kv);
            if !state.token_ids.is_empty() {
                body.insert("prompt".to_owned(), json!(state.token_ids));
            }
        },
        WireFormat::Generate => set_generate_transfer(&mut body, kv, None)?,
    }
    Ok(Value::Object(body))
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::*;
    use crate::inference::llmd::state::{PlaceholderRange, Topology};
    use crate::inference::llmd::{EcConnector, KvConnector};

    fn state(format: WireFormat) -> CoordinatorState {
        CoordinatorState {
            original_path: format.path().to_owned(),
            body: serde_json::from_value(json!({
                "model": "model-a", "messages": [], "max_tokens": 16, "stream": true
            }))
            .unwrap(),
            topology: Topology::FullyDisaggregated,
            format,
            token_ids: vec![10, 11, 12],
            multimodal: vec![MultimodalEntry {
                index: 0,
                hash: "hash-a".to_owned(),
                kwargs_data: "tensor-a".to_owned(),
                placeholder: PlaceholderRange { offset: 1, length: 2 },
            }],
            ec_transfer_params: serde_json::from_value(json!({"hash-a": {"peer_host": "enc"}})).unwrap(),
            kv_transfer_params: serde_json::from_value(json!({"remote_host": "pre"})).unwrap(),
            ec_connector: EcConnector::EcNixl,
            kv_connector: KvConnector::KvNixl,
        }
    }

    #[test]
    fn chat_prefill_has_tokens_and_transfer_params() {
        let body = prefill_body(&state(WireFormat::ChatCompletions)).unwrap();
        assert_eq!(body["max_tokens"], 1);
        assert_eq!(body["stream"], false);
        assert_eq!(body["tokens"]["token_ids"], json!([10, 11, 12]));
        assert_eq!(body["tokens"]["features"]["kwargs_data"]["image"][0], "tensor-a");
        assert_eq!(body["ec_transfer_params"]["hash-a"]["peer_host"], "enc");
    }

    #[test]
    fn generate_prefill_nests_transfer_params() {
        let body = prefill_body(&state(WireFormat::Generate)).unwrap();
        assert_eq!(body["sampling_params"]["max_tokens"], 1);
        assert_eq!(
            body["sampling_params"]["extra_args"]["kv_transfer_params"]["do_remote_decode"],
            true
        );
        assert_eq!(
            body["sampling_params"]["extra_args"]["ec_transfer_params"]["hash-a"]["peer_host"],
            "enc"
        );
    }

    #[test]
    fn decode_preserves_client_output_controls() {
        let body = decode_body(&state(WireFormat::ChatCompletions)).unwrap();
        assert_eq!(body["max_tokens"], 16);
        assert_eq!(body["stream"], true);
        assert_eq!(body["kv_transfer_params"]["do_remote_prefill"], true);
    }

    #[test]
    fn generate_decode_translates_openai_input_to_native_tokens() {
        let body = decode_body_with(
            &state(WireFormat::ChatCompletions),
            WireFormat::Generate,
            KvConnector::KvNixl,
        )
        .unwrap();
        assert_eq!(body["model"], "model-a");
        assert_eq!(body["token_ids"], json!([10, 11, 12]));
        assert_eq!(body["sampling_params"]["max_tokens"], 16);
        assert_eq!(body["features"]["mm_hashes"]["image"][0], "hash-a");
        assert!(body.get("messages").is_none());
        assert!(body["features"].get("kwargs_data").is_none());
        assert_eq!(
            body["sampling_params"]["extra_args"]["kv_transfer_params"]["do_remote_prefill"],
            true
        );
    }
}
