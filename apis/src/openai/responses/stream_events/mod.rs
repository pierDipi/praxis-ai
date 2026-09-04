// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Praxis Contributors

//! Accumulates state from native Responses API SSE event streams.
//!
//! Parses backend SSE chunks using [`SseFrameParser`], dispatches
//! typed events to update [`ResponsesState`] in request extensions.
//! The response body passes through unchanged.
//!
//! [`SseFrameParser`]: crate::openai::sse::SseFrameParser
//! [`ResponsesState`]: super::state::ResponsesState

pub(crate) mod accumulator;
mod config;

use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use praxis_filter::{
    BodyAccess, BodyMode, FilterAction, FilterError, HttpFilter, HttpFilterContext, parse_filter_config,
};
use tracing::{debug, trace, warn};

#[cfg(test)]
use self::accumulator::accumulate_response_object;
use self::{accumulator::accumulate_event, config::StreamEventsConfig};
use crate::{
    classifier::is_responses_create,
    is_event_stream_content_type,
    openai::sse::{SseFrameParser, SseParseError, SseParserConfig, responses::ResponsesEvent},
};

/// Completion state observed while parsing a Responses SSE stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CompletionState {
    /// No completion signal has been observed.
    Open,
    /// A terminal lifecycle event was observed.
    TerminalLifecycle,
    /// A stream-level error event was observed.
    Error,
}

/// Per-request parser and accumulation state.
pub(super) struct StreamEventsState {
    /// Byte-level SSE frame parser.
    frame_parser: SseFrameParser,
    /// Raw bytes retained until the next complete SSE event boundary.
    wire_buffer: Vec<u8>,
    /// Maximum raw partial-event bytes retained for response rewriting.
    max_wire_buffer_bytes: usize,
    /// Locally consumed predecessor id restored on terminal response events.
    previous_response_id: Option<String>,
    /// Number of non-sentinel events parsed so far.
    event_count: usize,
    /// Maximum allowed event count.
    max_events: usize,
    /// Maximum allowed wall-clock time.
    timeout: Duration,
    /// Timestamp of first chunk.
    started_at: Option<Instant>,
    /// Timestamp when a terminal state was first observed.
    completed_at: Option<Instant>,
    /// Stream completion state (`Open` / `TerminalLifecycle` / `Error`).
    completion_state: CompletionState,
    /// Accumulated function-call argument deltas, keyed by item id or output index.
    tool_call_args: std::collections::HashMap<String, String>,
    /// Tool-call keys whose arguments exceeded the configured byte cap.
    rejected_tool_call_args: std::collections::HashSet<String>,
    /// Cap on accumulated bytes per tool-call argument string.
    max_tool_call_argument_bytes: usize,
}

/// Accumulates state from native Responses API SSE event streams.
///
/// # YAML
///
/// ```yaml
/// filter: openai_stream_events
/// # All fields optional:
/// # max_buffer_bytes: 10485760
/// # max_events: 100000
/// # timeout_secs: 300
/// # max_tool_call_argument_bytes: 1048576
/// ```
pub struct OpenaiStreamEventsFilter {
    /// Configuration for the SSE frame parser.
    parser_config: SseParserConfig,
    /// Cap on accumulated bytes per tool-call argument string.
    max_tool_call_argument_bytes: usize,
}

impl OpenaiStreamEventsFilter {
    /// Create a filter from parsed YAML config.
    ///
    /// # Errors
    ///
    /// Returns [`FilterError`] if the YAML config is invalid.
    pub fn from_config(config: &serde_yaml::Value) -> Result<Box<dyn HttpFilter>, FilterError> {
        let cfg: StreamEventsConfig = parse_filter_config("openai_stream_events", config)?;
        cfg.validate()?;
        Ok(Box::new(Self {
            parser_config: cfg.to_parser_config(),
            max_tool_call_argument_bytes: cfg.max_tool_call_argument_bytes(),
        }))
    }

    /// Whether per-request parser state has been installed.
    fn is_armed(ctx: &HttpFilterContext<'_>) -> bool {
        ctx.get_filter_state::<StreamEventsState>().is_some()
    }
}

#[async_trait]
impl HttpFilter for OpenaiStreamEventsFilter {
    fn name(&self) -> &'static str {
        "openai_stream_events"
    }

    fn request_body_access(&self) -> BodyAccess {
        BodyAccess::None
    }

    fn request_body_mode(&self) -> BodyMode {
        BodyMode::Stream
    }

    fn response_body_access(&self) -> BodyAccess {
        BodyAccess::ReadWrite
    }

    fn response_body_mode(&self) -> BodyMode {
        BodyMode::Stream
    }

    async fn on_request(&self, ctx: &mut HttpFilterContext<'_>) -> Result<FilterAction, FilterError> {
        let is_responses = is_responses_create(&ctx.request.method, ctx.request.uri.path())
            && ctx.get_metadata("openai_responses_format.format") == Some("openai_responses");
        let is_streaming = ctx.get_metadata("openai_responses_format.stream") == Some("true");

        if is_responses && is_streaming {
            trace!("arming stream_events for streaming Responses API request");
            ctx.insert_filter_state(StreamEventsState {
                frame_parser: SseFrameParser::new(self.parser_config.max_buffer_bytes),
                wire_buffer: Vec::new(),
                max_wire_buffer_bytes: self.parser_config.max_buffer_bytes,
                previous_response_id: None,
                event_count: 0,
                max_events: self.parser_config.max_events,
                timeout: self.parser_config.timeout,
                started_at: None,
                completed_at: None,
                completion_state: CompletionState::Open,
                tool_call_args: std::collections::HashMap::new(),
                rejected_tool_call_args: std::collections::HashSet::new(),
                max_tool_call_argument_bytes: self.max_tool_call_argument_bytes,
            });
        }

        Ok(FilterAction::Continue)
    }

    async fn on_response(&self, ctx: &mut HttpFilterContext<'_>) -> Result<FilterAction, FilterError> {
        if !Self::is_armed(ctx) {
            return Ok(FilterAction::Continue);
        }

        if !is_success_sse_response(ctx) {
            debug!("disarming stream_events: response is not 2xx text/event-stream");
            ctx.remove_filter_state::<StreamEventsState>();
            return Ok(FilterAction::Continue);
        }

        let previous_response_id = response_has_identity_encoding(ctx).then(|| {
            ctx.extensions
                .get::<crate::openai::responses::state::ResponsesState>()
                .and_then(|state| {
                    state
                        .history_rehydrated
                        .then(|| state.previous_response_id.clone())
                        .flatten()
                })
        }).flatten();
        if let Some(state) = ctx.get_filter_state_mut::<StreamEventsState>() {
            state.previous_response_id = previous_response_id;
        }
        if ctx
            .get_filter_state::<StreamEventsState>()
            .is_some_and(|state| state.previous_response_id.is_some())
        {
            prepare_rewritten_stream_headers(ctx);
        }

        Ok(FilterAction::Continue)
    }

    fn on_response_body(
        &self,
        ctx: &mut HttpFilterContext<'_>,
        body: &mut Option<Bytes>,
        end_of_stream: bool,
    ) -> Result<FilterAction, FilterError> {
        if !Self::is_armed(ctx) {
            debug!("stream_events not armed, passing through");
            return Ok(FilterAction::Continue);
        }

        process_chunk(ctx, body, end_of_stream);

        if end_of_stream {
            validate_stream_end(ctx);
        }

        Ok(FilterAction::Continue)
    }
}

/// Parse SSE frames and accumulate state without modifying the body.
fn process_chunk(ctx: &mut HttpFilterContext<'_>, body: &mut Option<Bytes>, end_of_stream: bool) {
    let Some(mut state) = ctx.remove_filter_state::<StreamEventsState>() else {
        return;
    };

    if let Some(bytes) = body.as_ref() {
        let now = Instant::now();
        state.started_at.get_or_insert(now);

        if let Err(e) = parse_and_accumulate(&mut state, ctx, bytes, now) {
            warn!(error = %e, "SSE parse error in stream_events");
            ctx.set_metadata("responses.stream_parse_error", "true".to_owned());
        }
    }

    if state.previous_response_id.is_some() {
        rewrite_stream_chunk(&mut state, body, end_of_stream);
    }

    ctx.insert_filter_state(state);
}

/// Hold at most one partial event and rewrite completed terminal events.
fn rewrite_stream_chunk(state: &mut StreamEventsState, body: &mut Option<Bytes>, end_of_stream: bool) {
    if let Some(bytes) = body.as_ref() {
        state.wire_buffer.extend_from_slice(bytes);
    } else if !end_of_stream {
        return;
    }
    let completed_len = completed_sse_prefix_len(&state.wire_buffer);
    let mut pending = state.wire_buffer.split_off(completed_len);
    let completed = std::mem::replace(&mut state.wire_buffer, std::mem::take(&mut pending));

    let mut output = rewrite_completed_events(&completed, state.previous_response_id.as_deref());
    if end_of_stream && !state.wire_buffer.is_empty() {
        output.extend_from_slice(&state.wire_buffer);
        state.wire_buffer.clear();
    } else if state.wire_buffer.len() > state.max_wire_buffer_bytes {
        warn!(
            buffered_bytes = state.wire_buffer.len(),
            limit = state.max_wire_buffer_bytes,
            "stream response rewrite buffer exceeded limit; disabling rewrite"
        );
        output.extend_from_slice(&state.wire_buffer);
        state.wire_buffer.clear();
        state.previous_response_id = None;
    }
    *body = Some(Bytes::from(output));
}

/// Return the byte length through the last complete SSE event boundary.
fn completed_sse_prefix_len(bytes: &[u8]) -> usize {
    let mut consumed = 0;
    while let Some(event_len) = first_sse_event_len(&bytes[consumed..]) {
        consumed += event_len;
    }
    consumed
}

fn first_sse_event_len(bytes: &[u8]) -> Option<usize> {
    let mut line_start = 0;
    let mut index = 0;
    while index < bytes.len() {
        let terminator_len = match bytes[index] {
            b'\n' => 1,
            b'\r' if bytes.get(index + 1) == Some(&b'\n') => 2,
            b'\r' => 1,
            _ => {
                index += 1;
                continue;
            },
        };
        if index == line_start {
            return Some(index + terminator_len);
        }
        index += terminator_len;
        line_start = index;
    }
    None
}

/// Rewrite response lifecycle payloads while leaving every other event byte-exact.
fn rewrite_completed_events(bytes: &[u8], previous_response_id: Option<&str>) -> Vec<u8> {
    let Some(previous_response_id) = previous_response_id else {
        return bytes.to_vec();
    };
    let mut output = Vec::with_capacity(bytes.len());
    let mut start = 0;
    while start < bytes.len() {
        let Some(relative_end) = first_sse_event_len(&bytes[start..]) else {
            output.extend_from_slice(&bytes[start..]);
            break;
        };
        let end = start + relative_end;
        let raw_event = &bytes[start..end];
        output.extend_from_slice(&rewrite_response_event(raw_event, previous_response_id));
        start = end;
    }
    output
}

fn rewrite_response_event(raw_event: &[u8], previous_response_id: &str) -> Vec<u8> {
    let mut parser = SseFrameParser::new(raw_event.len());
    let Ok(frames) = parser.parse_chunk(raw_event) else {
        return raw_event.to_vec();
    };
    let Some(frame) = frames.first() else {
        return raw_event.to_vec();
    };
    let Ok(mut payload) = serde_json::from_slice::<serde_json::Value>(&frame.data) else {
        return raw_event.to_vec();
    };
    let event_type = payload.get("type").and_then(serde_json::Value::as_str);
    if !matches!(
        event_type,
        Some(
            "response.created"
                | "response.queued"
                | "response.in_progress"
                | "response.completed"
                | "response.incomplete"
                | "response.failed"
        )
    ) {
        return raw_event.to_vec();
    }
    let response = if payload.get("response").is_some() {
        &mut payload["response"]
    } else {
        &mut payload
    };
    let Some(response) = response.as_object_mut() else {
        return raw_event.to_vec();
    };
    response.insert(
        "previous_response_id".to_owned(),
        serde_json::Value::String(previous_response_id.to_owned()),
    );
    let Ok(data) = serde_json::to_vec(&payload) else {
        return raw_event.to_vec();
    };
    replace_sse_data(raw_event, &data)
}

/// Replace one event's possibly multi-line data field and preserve all other fields.
fn replace_sse_data(raw_event: &[u8], data: &[u8]) -> Vec<u8> {
    let mut output = Vec::with_capacity(raw_event.len() + data.len());
    let mut start = 0;
    let mut wrote_data = false;
    while start < raw_event.len() {
        let Some(relative_end) = raw_event[start..].iter().position(|byte| matches!(byte, b'\n' | b'\r')) else {
            output.extend_from_slice(&raw_event[start..]);
            break;
        };
        let line_end = start + relative_end;
        let terminator_end = if raw_event[line_end] == b'\r' && raw_event.get(line_end + 1) == Some(&b'\n') {
            line_end + 2
        } else {
            line_end + 1
        };
        let line = &raw_event[start..line_end];
        if line == b"data" || line.starts_with(b"data:") {
            if !wrote_data {
                output.extend_from_slice(b"data: ");
                output.extend_from_slice(data);
                output.extend_from_slice(&raw_event[line_end..terminator_end]);
                wrote_data = true;
            }
        } else {
            output.extend_from_slice(&raw_event[start..terminator_end]);
        }
        start = terminator_end;
    }
    output
}

/// Parse frames from raw bytes and accumulate events.
fn parse_and_accumulate(
    state: &mut StreamEventsState,
    ctx: &mut HttpFilterContext<'_>,
    bytes: &Bytes,
    now: Instant,
) -> Result<(), SseParseError> {
    check_timeout(state, now)?;

    let frames = state.frame_parser.parse_chunk_with_counted_event_limit(
        bytes,
        state.event_count,
        state.max_events,
        |frame| frame.data != b"[DONE]",
    )?;

    for frame in &frames {
        if frame.data == b"[DONE]" {
            continue;
        }

        state.event_count += 1;
        let event = ResponsesEvent::from_frame(frame)?;
        record_completion(state, &event, now)?;
        accumulate_event(ctx, state, &event);
    }

    Ok(())
}

/// Check whether the stream has exceeded its wall-clock timeout.
fn check_timeout(state: &StreamEventsState, now: Instant) -> Result<(), SseParseError> {
    let Some(started_at) = state.started_at else {
        return Ok(());
    };
    let elapsed = now.duration_since(started_at);
    if elapsed > state.timeout {
        return Err(SseParseError::Timeout {
            elapsed,
            limit: state.timeout,
        });
    }
    Ok(())
}

/// Record whether an event signals stream completion.
fn record_completion(state: &mut StreamEventsState, event: &ResponsesEvent, now: Instant) -> Result<(), SseParseError> {
    if matches!(event, ResponsesEvent::Error(_)) {
        if state.completion_state == CompletionState::Error {
            return Err(SseParseError::EventAfterTerminal {
                event_type: event.event_type().to_owned(),
            });
        }
        mark_complete(state, CompletionState::Error, now);
        return Ok(());
    }

    if state.completion_state != CompletionState::Open {
        return Err(SseParseError::EventAfterTerminal {
            event_type: event.event_type().to_owned(),
        });
    }

    if event.is_terminal() {
        mark_complete(state, CompletionState::TerminalLifecycle, now);
    }

    Ok(())
}

/// Record the first terminal-state timestamp while allowing stronger
/// states to replace weaker ones.
fn mark_complete(state: &mut StreamEventsState, new_state: CompletionState, now: Instant) {
    state.completion_state = new_state;
    state.completed_at.get_or_insert(now);
}

/// Check that the SSE stream terminated with a terminal event.
fn validate_stream_end(ctx: &mut HttpFilterContext<'_>) {
    if let Some(state) = ctx.get_filter_state::<StreamEventsState>() {
        let checked_at = state.completed_at.unwrap_or_else(Instant::now);
        if let Err(e) = check_timeout(state, checked_at) {
            warn!(error = %e, "stream did not terminate cleanly");
            ctx.set_metadata("responses.stream_incomplete", "true".to_owned());
        } else if state.completion_state == CompletionState::Open {
            warn!("stream did not terminate cleanly: missing terminal event");
            ctx.set_metadata("responses.stream_incomplete", "true".to_owned());
        }
    }
    debug!("stream_events processing complete");
}

/// Whether the response is a successful `text/event-stream` response.
fn is_success_sse_response(ctx: &HttpFilterContext<'_>) -> bool {
    let Some(resp) = ctx.response_header.as_ref() else {
        return true;
    };

    if !resp.status.is_success() {
        return false;
    }

    resp.headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(is_event_stream_content_type)
}

fn response_has_identity_encoding(ctx: &HttpFilterContext<'_>) -> bool {
    ctx.response_header.as_ref().is_none_or(|response| {
        response
            .headers
            .get(http::header::CONTENT_ENCODING)
            .is_none_or(|value| value.to_str().is_ok_and(|encoding| encoding.eq_ignore_ascii_case("identity")))
    })
}

/// Remove representation metadata invalidated by rewriting terminal SSE events.
fn prepare_rewritten_stream_headers(ctx: &mut HttpFilterContext<'_>) {
    let Some(response) = &mut ctx.response_header else {
        return;
    };
    response.headers.remove(http::header::CONTENT_LENGTH);
    response.headers.remove(http::header::CONTENT_ENCODING);
    response.headers.remove(http::header::CONTENT_RANGE);
    response.headers.remove(http::header::ETAG);
    for header in ["content-digest", "content-md5", "digest", "repr-digest"] {
        response.headers.remove(header);
    }
    ctx.response_headers_modified = true;
}

#[cfg(test)]
mod tests;
