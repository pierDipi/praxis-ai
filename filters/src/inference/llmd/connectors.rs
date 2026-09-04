// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Praxis Contributors

//! llm-d disaggregated inference transfer parameter construction.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

/// Supported KV transfer protocols.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum KvConnector {
    /// NIXL peer-to-peer KV transfer.
    KvNixl,
    /// Shared storage, with no peer descriptor.
    #[default]
    KvSharedStorage,
}

/// Supported encoder-cache transfer protocols.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum EcConnector {
    /// NIXL peer-to-peer embedding transfer.
    EcNixl,
    /// Shared storage keyed by multimodal hash.
    #[default]
    EcSharedStorage,
}

impl KvConnector {
    /// Build the parameters sent to a prefill worker.
    #[must_use]
    pub fn prefill_params(self) -> Value {
        match self {
            Self::KvNixl => json!({
                "do_remote_decode": true,
                "do_remote_prefill": false,
                "remote_engine_id": null,
                "remote_block_ids": null,
                "remote_host": null,
                "remote_port": null
            }),
            Self::KvSharedStorage => json!({
                "do_remote_decode": true,
                "do_remote_prefill": false
            }),
        }
    }

    /// Build decode parameters from the descriptor returned by prefill.
    #[must_use]
    pub fn decode_params(self, returned: &Map<String, Value>) -> Value {
        let mut params = returned.clone();
        params.insert("do_remote_decode".to_owned(), Value::Bool(false));
        params.insert("do_remote_prefill".to_owned(), Value::Bool(true));
        Value::Object(params)
    }
}

impl EcConnector {
    /// Merge encoder response descriptors for a later prefill request.
    ///
    /// # Errors
    ///
    /// Returns an error when two workers return different descriptors for the
    /// same multimodal hash.
    pub fn merge_encode_responses(
        self,
        responses: impl IntoIterator<Item = Map<String, Value>>,
    ) -> Result<Map<String, Value>, String> {
        if self == Self::EcSharedStorage {
            return Ok(Map::new());
        }

        let mut merged = HashMap::<String, Value>::new();
        for response in responses {
            let Some(Value::Object(params)) = response.get("ec_transfer_params") else {
                continue;
            };
            for (hash, descriptor) in params {
                if descriptor.is_null() {
                    continue;
                }
                if let Some(previous) = merged.get(hash)
                    && previous != descriptor
                {
                    return Err(format!(
                        "ec_transfer_params: conflicting descriptors for mm_hash '{hash}'"
                    ));
                }
                merged.insert(hash.clone(), descriptor.clone());
            }
        }
        Ok(merged.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nixl_prefill_shape_matches_coordinator_contract() {
        assert_eq!(
            KvConnector::KvNixl.prefill_params(),
            json!({
                "do_remote_decode": true,
                "do_remote_prefill": false,
                "remote_engine_id": null,
                "remote_block_ids": null,
                "remote_host": null,
                "remote_port": null
            })
        );
    }

    #[test]
    fn decode_preserves_returned_descriptor() {
        let returned = serde_json::from_value(json!({
            "remote_host": "10.0.0.2",
            "remote_port": 14579
        }))
        .unwrap();
        assert_eq!(
            KvConnector::KvNixl.decode_params(&returned),
            json!({
                "remote_host": "10.0.0.2",
                "remote_port": 14579,
                "do_remote_decode": false,
                "do_remote_prefill": true
            })
        );
    }

    #[test]
    fn nixl_ec_merges_equal_descriptors() {
        let response: Map<String, Value> = serde_json::from_value(json!({
            "ec_transfer_params": {"hash-a": {"peer_host": "encoder-a"}}
        }))
        .unwrap();
        let merged = EcConnector::EcNixl
            .merge_encode_responses([response.clone(), response])
            .unwrap();
        assert_eq!(merged["hash-a"], json!({"peer_host": "encoder-a"}));
    }

    #[test]
    fn nixl_ec_rejects_conflicting_descriptors() {
        let first = serde_json::from_value(json!({
            "ec_transfer_params": {"hash-a": {"peer_host": "encoder-a"}}
        }))
        .unwrap();
        let second = serde_json::from_value(json!({
            "ec_transfer_params": {"hash-a": {"peer_host": "encoder-b"}}
        }))
        .unwrap();
        let error = EcConnector::EcNixl.merge_encode_responses([first, second]).unwrap_err();
        assert!(error.contains("hash-a"));
    }

    #[test]
    fn shared_storage_ignores_encoder_descriptors() {
        let response = serde_json::from_value(json!({
            "ec_transfer_params": {"hash-a": {"peer_host": "encoder-a"}}
        }))
        .unwrap();
        assert!(
            EcConnector::EcSharedStorage
                .merge_encode_responses([response])
                .unwrap()
                .is_empty()
        );
    }
}
