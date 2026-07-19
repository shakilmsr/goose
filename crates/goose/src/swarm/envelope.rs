//! Structured output envelope for swarm subtasks.
//!
//! Tier 1: every subtask recipe carries a fixed `{status, output, notes}` JSON
//! response schema (enforced by Goose's `final_output` tool, which validates
//! against it), so the kernel gets a real success/failure signal instead of
//! treating any returned text as success — the SwarmOS lesson that a model's
//! prose cannot be trusted to signal whether the workflow graph should proceed.
//!
//! Tier 2: a subtask may carry its own task-specific `output_schema` (authored by
//! the decomposer, validated before acceptance). It nests as the envelope's
//! `output` property, so the status signal survives while `output` becomes typed
//! JSON that downstream subtasks consume structurally instead of as prose.

use serde::Deserialize;
use serde_json::Value;

pub fn envelope_schema() -> Value {
    envelope_schema_for(None)
}

/// The envelope schema, with `output` specialized to the subtask's own schema
/// when one is declared. `None` yields the plain Tier-1 string envelope.
pub fn envelope_schema_for(output_schema: Option<&Value>) -> Value {
    let output_property = match output_schema {
        Some(schema) => {
            let mut schema = schema.clone();
            if let Value::Object(map) = &mut schema {
                map.entry("description").or_insert_with(|| {
                    Value::String(
                        "The structured result of this subtask. Downstream agents consume it \
                         verbatim as JSON."
                            .to_string(),
                    )
                });
            }
            schema
        }
        None => serde_json::json!({
            "type": "string",
            "description": "The complete, self-contained result of this subtask. Downstream agents consume this verbatim."
        }),
    };
    serde_json::json!({
        "type": "object",
        "required": ["status", "output"],
        "properties": {
            "status": {
                "type": "string",
                "enum": ["success", "failed"],
                "description": "Whether this subtask genuinely accomplished its instruction. Report 'failed' if you could not complete it — do not dress a failure up as success."
            },
            "output": output_property,
            "notes": {
                "type": "string",
                "description": "Caveats, assumptions, or (on failure) what went wrong and what was attempted."
            }
        }
    })
}

#[derive(Debug, PartialEq)]
pub enum SubtaskOutcome {
    Success(String),
    Failed(String),
}

/// A critic's routing decision in a review loop. `Revise` carries the feedback the
/// worker should address on its next pass.
#[derive(Debug, Clone, PartialEq)]
pub enum Verdict {
    Accept,
    Revise(String),
}

/// The critic envelope: a review-loop critic returns an accept/revise verdict plus
/// actionable feedback instead of the plain worker envelope. `status` is retained
/// so a critic that genuinely could not perform the review still fails the node
/// (via the normal Tier-1 path) rather than being read as a verdict.
pub fn critic_envelope_schema() -> Value {
    serde_json::json!({
        "type": "object",
        "required": ["status", "verdict"],
        "properties": {
            "status": {
                "type": "string",
                "enum": ["success", "failed"],
                "description": "Whether you were able to perform the review at all. Report 'failed' only if you could not evaluate the work; a completed review that finds problems is still 'success' with verdict 'revise'."
            },
            "verdict": {
                "type": "string",
                "enum": ["accept", "revise"],
                "description": "'accept' if the worker's output meets the task's requirements; 'revise' if it must be corrected and re-run."
            },
            "feedback": {
                "type": "string",
                "description": "On 'revise', the specific, actionable corrections the worker must make. Required when revising; the worker sees only this text."
            },
            "assessment": {
                "type": "string",
                "description": "Brief reasoning for the verdict."
            }
        }
    })
}

#[derive(Deserialize)]
struct RawCriticEnvelope {
    #[serde(default)]
    status: String,
    #[serde(default)]
    verdict: String,
    #[serde(default)]
    feedback: String,
    #[serde(default)]
    assessment: String,
}

/// Interpret a critic subagent's raw output into a node outcome plus a verdict. A
/// `status=failed` envelope means the critic could not review — the node fails and
/// carries no verdict (the loop stalls, surfacing a broken critic). Otherwise the
/// `verdict` field routes the loop: `revise` carries the feedback, `accept`
/// proceeds. Anything unparseable degrades to `accept` (fail-open toward
/// termination — defaulting to revise would burn revision cycles, and the loop is
/// bounded regardless), preserving the raw text as the outcome.
pub fn interpret_critic_output(raw: &str) -> (SubtaskOutcome, Option<Verdict>) {
    match serde_json::from_str::<RawCriticEnvelope>(raw.trim()) {
        Ok(env) if env.status == "failed" => {
            let reason = if env.assessment.is_empty() {
                "critic reported it could not perform the review".to_string()
            } else {
                env.assessment
            };
            (SubtaskOutcome::Failed(reason), None)
        }
        Ok(env) if env.verdict == "revise" => {
            let feedback = if env.feedback.is_empty() {
                if env.assessment.is_empty() {
                    "the reviewer requested revisions but gave no specifics".to_string()
                } else {
                    env.assessment.clone()
                }
            } else {
                env.feedback
            };
            let summary = if env.assessment.is_empty() {
                "revise".to_string()
            } else {
                env.assessment
            };
            (
                SubtaskOutcome::Success(summary),
                Some(Verdict::Revise(feedback)),
            )
        }
        Ok(env) if env.verdict == "accept" => {
            let summary = if env.assessment.is_empty() {
                "accept".to_string()
            } else {
                env.assessment
            };
            (SubtaskOutcome::Success(summary), Some(Verdict::Accept))
        }
        _ => (
            SubtaskOutcome::Success(raw.to_string()),
            Some(Verdict::Accept),
        ),
    }
}

#[derive(Deserialize)]
struct RawEnvelope {
    status: String,
    #[serde(default)]
    output: Value,
    #[serde(default)]
    notes: String,
}

fn output_text(output: Value) -> String {
    match output {
        Value::String(s) => s,
        Value::Null => String::new(),
        other => serde_json::to_string(&other).unwrap_or_default(),
    }
}

/// Interpret a subagent's raw output. A schema-validated envelope decides the
/// outcome — a structured (non-string) `output` is passed on as compact JSON
/// text. Anything that isn't an envelope (the agent bypassed the final-output
/// tool and Goose fell back to its last text message) degrades to success with
/// the raw text, so a schema hiccup never fails an otherwise good result.
pub fn interpret_subtask_output(raw: &str) -> SubtaskOutcome {
    match serde_json::from_str::<RawEnvelope>(raw.trim()) {
        Ok(envelope) if envelope.status == "failed" => {
            let reason = if envelope.notes.is_empty() {
                output_text(envelope.output)
            } else {
                envelope.notes
            };
            SubtaskOutcome::Failed(if reason.is_empty() {
                "subtask reported failure without details".to_string()
            } else {
                reason
            })
        }
        Ok(envelope) => SubtaskOutcome::Success(output_text(envelope.output)),
        Err(_) => SubtaskOutcome::Success(raw.to_string()),
    }
}
