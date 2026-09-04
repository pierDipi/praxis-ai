// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Praxis Contributors

//! Input normalization and rendering for llm-d coordinator requests.

use std::{
    collections::HashSet,
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    sync::Arc,
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use reqwest::{Client, StatusCode, Url, header::CONTENT_TYPE};
use serde_json::{Map, Value};

use super::state::{MultimodalEntry, PlaceholderRange, PreprocessingLimits, WireFormat};

const MAX_REDIRECTS: usize = 5;

#[derive(Debug)]
pub(crate) struct PreprocessError {
    pub(crate) client: bool,
    pub(crate) message: String,
}

impl PreprocessError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            client: true,
            message: message.into(),
        }
    }

    fn upstream(message: impl Into<String>) -> Self {
        Self {
            client: false,
            message: message.into(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MediaConfig {
    pub(crate) timeout: Duration,
    pub(crate) max_concurrent_downloads: usize,
    pub(crate) max_entries: usize,
    pub(crate) max_bytes: usize,
    pub(crate) allow_private_networks: bool,
    pub(crate) allowed_domains: Vec<String>,
}

fn image_urls_mut(body: &mut Map<String, Value>) -> Vec<&mut Value> {
    let mut result = Vec::new();
    let Some(messages) = body.get_mut("messages").and_then(Value::as_array_mut) else {
        return result;
    };
    for message in messages {
        let Some(parts) = message.get_mut("content").and_then(Value::as_array_mut) else {
            continue;
        };
        for part in parts {
            let Some(part) = part.as_object_mut() else { continue };
            if part.get("type").and_then(Value::as_str) != Some("image_url") {
                continue;
            }
            let Some(image) = part.get_mut("image_url") else {
                continue;
            };
            if let Some(object) = image.as_object_mut()
                && let Some(url) = object.get_mut("url")
            {
                result.push(url);
            }
        }
    }
    result
}

fn validate_content_type(value: &str) -> Result<(), PreprocessError> {
    let mime = value.split(';').next().unwrap_or_default().trim();
    if matches!(mime, "image/jpeg" | "image/png" | "image/gif" | "image/webp") {
        Ok(())
    } else {
        Err(PreprocessError::bad_request(format!(
            "unsupported image content type {mime:?}"
        )))
    }
}

fn parse_data_uri(value: &str, max_bytes: usize) -> Result<(), PreprocessError> {
    let Some(rest) = value.strip_prefix("data:") else {
        return Err(PreprocessError::bad_request("media URL must be http, https, or data"));
    };
    let Some((metadata, encoded)) = rest.split_once(',') else {
        return Err(PreprocessError::bad_request("malformed data URI"));
    };
    let Some(mime) = metadata.strip_suffix(";base64") else {
        return Err(PreprocessError::bad_request("image data URI must use base64 encoding"));
    };
    validate_content_type(mime)?;
    let decoded = STANDARD
        .decode(encoded)
        .map_err(|_| PreprocessError::bad_request("image data URI has invalid base64"))?;
    if decoded.len() > max_bytes {
        return Err(PreprocessError::bad_request("image data exceeds max_download_bytes"));
    }
    Ok(())
}

fn permitted_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let octets = ip.octets();
            !(ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_broadcast()
                || ip.is_documentation()
                || ip.is_unspecified()
                || ip.is_multicast()
                || octets[0] == 0
                || (octets[0] == 100 && (64..=127).contains(&octets[1]))
                || (octets[0] == 198 && (18..=19).contains(&octets[1])))
        },
        IpAddr::V6(ip) => {
            if let Some(mapped) = ip.to_ipv4_mapped() {
                return permitted_ip(IpAddr::V4(mapped));
            }
            !(ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_multicast()
                || ip.is_unique_local()
                || ip.is_unicast_link_local()
                || (ip.segments()[0] == 0x2001 && ip.segments()[1] == 0x0db8))
        },
    }
}

fn domain_allowed(host: &str, allowed: &[String]) -> bool {
    allowed.is_empty()
        || allowed.iter().any(|domain| {
            let domain = domain.trim_start_matches('.');
            host.eq_ignore_ascii_case(domain)
                || host
                    .to_ascii_lowercase()
                    .ends_with(&format!(".{domain}").to_ascii_lowercase())
        })
}

async fn pinned_client(url: &Url, config: &MediaConfig) -> Result<Client, PreprocessError> {
    let host = url
        .host_str()
        .ok_or_else(|| PreprocessError::bad_request("media URL has no host"))?;
    if !domain_allowed(host, &config.allowed_domains) {
        return Err(PreprocessError::bad_request("media URL domain is not allowed"));
    }
    let port = url
        .port_or_known_default()
        .ok_or_else(|| PreprocessError::bad_request("media URL has no port"))?;
    let host_owned = host.to_owned();
    let resolved = tokio::task::spawn_blocking(move || {
        (host_owned.as_str(), port)
            .to_socket_addrs()
            .map(|iter| iter.collect::<Vec<_>>())
    })
    .await
    .map_err(|_| PreprocessError::upstream("media DNS lookup failed"))?
    .map_err(|_| PreprocessError::upstream("media DNS lookup failed"))?;
    if resolved.is_empty() || (!config.allow_private_networks && resolved.iter().any(|addr| !permitted_ip(addr.ip()))) {
        return Err(PreprocessError::bad_request(
            "media URL resolves to a prohibited address",
        ));
    }
    let addrs = resolved
        .into_iter()
        .map(|addr| SocketAddr::new(addr.ip(), port))
        .collect::<Vec<_>>();
    Client::builder()
        .timeout(config.timeout)
        .redirect(reqwest::redirect::Policy::none())
        .resolve_to_addrs(host, &addrs)
        .build()
        .map_err(|_| PreprocessError::upstream("media HTTP client setup failed"))
}

async fn download(url: &str, config: &MediaConfig) -> Result<String, PreprocessError> {
    let mut url = Url::parse(url).map_err(|_| PreprocessError::bad_request("invalid media URL"))?;
    for redirects in 0..=MAX_REDIRECTS {
        if !matches!(url.scheme(), "http" | "https") {
            return Err(PreprocessError::bad_request("media URL scheme must be http or https"));
        }
        let client = pinned_client(&url, config).await?;
        let mut response = client
            .get(url.clone())
            .send()
            .await
            .map_err(|_| PreprocessError::upstream("media download failed"))?;
        if response.status().is_redirection() {
            if redirects == MAX_REDIRECTS {
                return Err(PreprocessError::bad_request("too many media redirects"));
            }
            let location = response
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| PreprocessError::bad_request("media redirect has no valid location"))?;
            url = url
                .join(location)
                .map_err(|_| PreprocessError::bad_request("invalid media redirect"))?;
            continue;
        }
        if !response.status().is_success() {
            return Err(PreprocessError::upstream(format!(
                "media download returned {}",
                response.status()
            )));
        }
        let mime = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_owned();
        validate_content_type(&mime)?;
        if response
            .content_length()
            .is_some_and(|length| length > config.max_bytes as u64)
        {
            return Err(PreprocessError::bad_request(
                "image content length exceeds max_download_bytes",
            ));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| PreprocessError::upstream("media response body failed"))?
        {
            if bytes.len().saturating_add(chunk.len()) > config.max_bytes {
                return Err(PreprocessError::bad_request("image data exceeds max_download_bytes"));
            }
            bytes.extend_from_slice(&chunk);
        }
        let mime = mime.split(';').next().unwrap_or("application/octet-stream");
        return Ok(format!("data:{mime};base64,{}", STANDARD.encode(bytes)));
    }
    unreachable!("redirect loop returns")
}

pub(crate) async fn normalize_media(
    body: &mut Map<String, Value>,
    config: &MediaConfig,
) -> Result<usize, PreprocessError> {
    let urls = image_urls_mut(body);
    if urls.len() > config.max_entries {
        return Err(PreprocessError::bad_request("too many multimodal entries"));
    }
    // Mutable JSON references cannot cross concurrent tasks. Download into an
    // ordered side vector, then move each normalized value into the body.
    let originals = urls
        .iter()
        .map(|value| value.as_str().map(str::to_owned))
        .collect::<Vec<_>>();
    let mut normalized = vec![String::new(); originals.len()];
    let semaphore = Arc::new(tokio::sync::Semaphore::new(config.max_concurrent_downloads));
    let mut tasks = tokio::task::JoinSet::new();
    for (index, original) in originals.into_iter().enumerate() {
        let value = original.ok_or_else(|| PreprocessError::bad_request("image_url.url must be a string"))?;
        let config = config.clone();
        let semaphore = Arc::clone(&semaphore);
        tasks.spawn(async move {
            let _permit = semaphore
                .acquire_owned()
                .await
                .map_err(|_| PreprocessError::upstream("media download cancelled"))?;
            let value = if value.starts_with("data:") {
                parse_data_uri(&value, config.max_bytes)?;
                value
            } else {
                download(&value, &config).await?
            };
            Ok::<_, PreprocessError>((index, value))
        });
    }
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok((index, value))) => normalized[index] = value,
            Ok(Err(error)) => {
                tasks.abort_all();
                return Err(error);
            },
            Err(_) => {
                tasks.abort_all();
                return Err(PreprocessError::upstream("media download task failed"));
            },
        }
    }
    for (target, value) in urls.into_iter().zip(normalized) {
        *target = Value::String(value);
    }
    Ok(originals_len(body))
}

fn originals_len(body: &mut Map<String, Value>) -> usize {
    image_urls_mut(body).len()
}

fn image_array<'a>(features: &'a Map<String, Value>, name: &str) -> Result<Option<&'a Vec<Value>>, PreprocessError> {
    let Some(value) = features.get(name) else {
        return Ok(None);
    };
    let object = value
        .as_object()
        .ok_or_else(|| PreprocessError::bad_request(format!("features.{name} must be an object")))?;
    object
        .get("image")
        .map(|value| {
            value
                .as_array()
                .ok_or_else(|| PreprocessError::bad_request(format!("features.{name}.image must be an array")))
        })
        .transpose()
}

pub(crate) fn parse_rendered(
    value: &Value,
    expected_media: Option<usize>,
    limits: PreprocessingLimits,
) -> Result<(Vec<u64>, Vec<MultimodalEntry>), PreprocessError> {
    let value = value
        .as_object()
        .ok_or_else(|| PreprocessError::bad_request("rendered request must be an object"))?;
    parse_rendered_map(value, expected_media, limits)
}

pub(crate) fn parse_rendered_map(
    value: &Map<String, Value>,
    expected_media: Option<usize>,
    limits: PreprocessingLimits,
) -> Result<(Vec<u64>, Vec<MultimodalEntry>), PreprocessError> {
    let tokens = value
        .get("token_ids")
        .and_then(Value::as_array)
        .ok_or_else(|| PreprocessError::bad_request("token_ids must be an array"))?;
    if tokens.len() > limits.max_total_tokens {
        return Err(PreprocessError::bad_request("too many token_ids"));
    }
    let token_ids = tokens
        .iter()
        .map(|token| {
            token
                .as_u64()
                .ok_or_else(|| PreprocessError::bad_request("token_ids must contain non-negative integers"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let Some(features) = value.get("features").filter(|value| !value.is_null()) else {
        if expected_media.unwrap_or(0) == 0 {
            return Ok((token_ids, Vec::new()));
        }
        return Err(PreprocessError::upstream("render response has no multimodal features"));
    };
    let features = features
        .as_object()
        .ok_or_else(|| PreprocessError::bad_request("features must be an object"))?;
    let hashes = image_array(features, "mm_hashes")?.cloned().unwrap_or_default();
    let placeholders = image_array(features, "mm_placeholders")?.cloned().unwrap_or_default();
    let kwargs = image_array(features, "kwargs_data")?.cloned();
    if expected_media.is_some_and(|expected| hashes.len() != expected)
        || placeholders.len() != hashes.len()
        || kwargs.as_ref().is_some_and(|values| values.len() != hashes.len())
    {
        return Err(PreprocessError::upstream(
            "render response multimodal cardinality mismatch",
        ));
    }
    let mut seen = HashSet::new();
    let mut previous_end = 0usize;
    let mut total_placeholders = 0usize;
    let mut entries = Vec::with_capacity(hashes.len());
    for index in 0..hashes.len() {
        let hash = hashes[index]
            .as_str()
            .filter(|hash| !hash.is_empty())
            .ok_or_else(|| PreprocessError::bad_request("mm_hashes must contain non-empty strings"))?;
        if !seen.insert(hash) {
            return Err(PreprocessError::upstream("render response contains duplicate mm_hash"));
        }
        let placeholder = placeholders[index]
            .as_object()
            .ok_or_else(|| PreprocessError::bad_request("mm_placeholders must contain objects"))?;
        let offset = placeholder
            .get("offset")
            .and_then(Value::as_u64)
            .and_then(|v| usize::try_from(v).ok())
            .ok_or_else(|| PreprocessError::bad_request("placeholder offset must be a non-negative integer"))?;
        let length = placeholder
            .get("length")
            .and_then(Value::as_u64)
            .and_then(|v| usize::try_from(v).ok())
            .ok_or_else(|| PreprocessError::bad_request("placeholder length must be a non-negative integer"))?;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| PreprocessError::bad_request("placeholder span overflows"))?;
        if offset < previous_end || offset >= token_ids.len() || end > token_ids.len() {
            return Err(PreprocessError::bad_request(
                "placeholder spans overlap or exceed token_ids",
            ));
        }
        previous_end = end;
        total_placeholders = total_placeholders
            .checked_add(length)
            .ok_or_else(|| PreprocessError::bad_request("placeholder total overflows"))?;
        if total_placeholders > limits.max_total_placeholder_tokens {
            return Err(PreprocessError::bad_request("too many placeholder tokens"));
        }
        let kwargs_data = kwargs
            .as_ref()
            .and_then(|values| values[index].as_str())
            .unwrap_or_default()
            .to_owned();
        entries.push(MultimodalEntry {
            index,
            hash: hash.to_owned(),
            kwargs_data,
            placeholder: PlaceholderRange { offset, length },
        });
    }
    Ok((token_ids, entries))
}

pub(crate) async fn render(
    client: &Client,
    base_url: &str,
    format: WireFormat,
    body: &Map<String, Value>,
    expected_media: usize,
    limits: PreprocessingLimits,
) -> Result<(Vec<u64>, Vec<MultimodalEntry>), PreprocessError> {
    let path = match format {
        WireFormat::ChatCompletions => "/v1/chat/completions/render",
        WireFormat::Completions => "/v1/completions/render",
        WireFormat::Generate => return parse_rendered_map(body, None, limits),
    };
    let response = client
        .post(format!("{}{path}", base_url.trim_end_matches('/')))
        .json(body)
        .send()
        .await
        .map_err(|_| PreprocessError::upstream("render request failed"))?;
    if response.status() != StatusCode::OK {
        return Err(PreprocessError::upstream(format!(
            "render returned {}",
            response.status()
        )));
    }
    let value: Value = response
        .json()
        .await
        .map_err(|_| PreprocessError::upstream("render returned invalid JSON"))?;
    let value = if format == WireFormat::Completions {
        value
            .as_array()
            .and_then(|values| values.first())
            .ok_or_else(|| PreprocessError::upstream("render returned invalid completions response"))?
    } else {
        &value
    };
    parse_rendered(value, Some(expected_media), limits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const LIMITS: PreprocessingLimits = PreprocessingLimits {
        max_total_tokens: 16,
        max_total_placeholder_tokens: 8,
    };

    #[test]
    fn validates_generate_features() {
        let value = json!({"token_ids":[1,2,3,4],"features":{"mm_hashes":{"image":["h"]},"mm_placeholders":{"image":[{"offset":1,"length":2}]},"kwargs_data":{"image":["tensor"]}}});
        let (_, entries) = parse_rendered(&value, None, LIMITS).unwrap();
        assert_eq!(entries[0].hash, "h");
        assert_eq!(entries[0].placeholder, PlaceholderRange { offset: 1, length: 2 });
    }

    #[test]
    fn rejects_overlapping_placeholders() {
        let value = json!({"token_ids":[1,2,3],"features":{"mm_hashes":{"image":["a","b"]},"mm_placeholders":{"image":[{"offset":0,"length":2},{"offset":1,"length":1}]}}});
        assert!(parse_rendered(&value, None, LIMITS).is_err());
    }

    #[test]
    fn validates_data_uri_size_and_type() {
        assert!(parse_data_uri("data:image/png;base64,AQID", 3).is_ok());
        assert!(parse_data_uri("data:text/plain;base64,AQID", 3).is_err());
        assert!(parse_data_uri("data:image/png;base64,AQID", 2).is_err());
    }

    #[test]
    fn domain_matching_does_not_accept_suffix_confusion() {
        assert!(domain_allowed("img.example.com", &["example.com".to_owned()]));
        assert!(!domain_allowed("badexample.com", &["example.com".to_owned()]));
    }

    #[test]
    fn blocks_non_public_and_ipv4_mapped_addresses() {
        for address in [
            "127.0.0.1",
            "169.254.169.254",
            "100.64.0.1",
            "198.18.0.1",
            "::1",
            "::ffff:127.0.0.1",
            "2001:db8::1",
        ] {
            assert!(!permitted_ip(address.parse().unwrap()), "{address}");
        }
        assert!(permitted_ip("93.184.216.34".parse().unwrap()));
    }
}
