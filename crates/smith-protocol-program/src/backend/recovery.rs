//! Resume admission and recoverable teardown (protocol/programs.md 5.8).
//! Reuses the stopped invocation's storage; never reads the session database.
//! A resumed assistant wait identifies a parked held turn (programs.md 5.9).
use super::{Backend, Phase, tail};
use smith_domain::llm;
use smith_protocol_llm::FromDomain;

pub(super) fn restartable(failure: llm::Failure) -> bool {
    match failure {
        llm::Failure::SessionGone
        | llm::Failure::Overloaded
        | llm::Failure::Unavailable
        | llm::Failure::RateLimited { .. }
        | llm::Failure::Unauthorized => true,
        llm::Failure::TimedOut { phase } => match phase {
            llm::Phase::Connect | llm::Phase::Handshake | llm::Phase::Head | llm::Phase::Idle => true,
            llm::Phase::Whole => false,
        },
        llm::Failure::Limit { .. }
        | llm::Failure::ApplicationLimit
        | llm::Failure::Protocol
        | llm::Failure::Cancelled
        | llm::Failure::ContextTooLong
        | llm::Failure::Invalid
        | llm::Failure::SignedOut
        | llm::Failure::Program { .. }
        | llm::Failure::Refused
        | llm::Failure::Unsupported
        | llm::Failure::Exhausted { .. } => false,
    }
}

impl Backend {
    pub(super) fn resume_input(&mut self) -> Option<()> {
        // A retry of an already recorded invocation has consumed this tail.
        // Only a newly restored window needs the parked-wait explanation.
        let restored = self.recorded.is_none() || self.recorded != self.running;
        let (prompt, capture) = match self.request.as_mut()? {
            FromDomain::Complete { prompt, observation, .. } => (prompt, observation.is_some()),
            FromDomain::Cancel { .. } | FromDomain::Yield { .. } => return None,
        };
        let prefix = if restored && parked_wait(prompt) {
            b"(The agent paused your last turn: the run was parked and has resumed.)\n".as_slice()
        } else {
            b"".as_slice()
        };
        let rendered = tail::render_observed(
            prompt,
            &self.ledger,
            &self.settings.adapter,
            self.settings.input_bytes,
            prefix,
            capture,
        )?;
        self.outgoing = Some(rendered.line);
        if let Some(text) = rendered.text {
            self.prompt_text_measure(skein_lib::Time::ZERO, text);
        }
        Some(())
    }

    pub(super) fn exit_failure(&self) -> llm::Failure {
        if self.phase == Phase::Starting
            && self.resuming
            && !self.stderr_wrapped
            && self.stderr.as_slice().starts_with(b"No conversation found with session ID")
        {
            return llm::Failure::SessionGone;
        }
        llm::Failure::Program { exit: self.exit }
    }
}

// Session restoration supplies the withheld wait's result in the user tail.
// Older waits are irrelevant once a later assistant message has been saved.
fn parked_wait(prompt: &llm::Prompt) -> bool {
    for message in prompt.messages.iter().rev() {
        if message.role == llm::Role::Assistant {
            for block in &message.content {
                match block {
                    llm::Block::ToolCall { name, .. } if name.as_ref() == b"wait" => return true,
                    llm::Block::ToolCall { .. }
                    | llm::Block::ToolResult { .. }
                    | llm::Block::Text { .. }
                    | llm::Block::Opaque { .. }
                    | llm::Block::Refusal { .. } => {}
                }
            }
            return false;
        }
    }
    false
}
