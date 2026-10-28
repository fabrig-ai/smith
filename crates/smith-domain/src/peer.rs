//! Peers: what the top level keeps of each conversation the run opens, from
//! its `Open` to its session's `Ended`, and the tickets that session carries.
//!
//! The session cannot name run asks, so each live decoded application call gets
//! one bounded peer ticket until dispatch or yield. Fixed offered descriptors
//! are finish/deliver/sub-agent/wait plus admitted opaque host declarations.
//! Aggregate decoded owning bytes and completion block count bound this map.
//! Concrete V2 results move through the ready list into session history; no
//! persistent root answer-ticket inventory survives for prompt replay. Historical
//! calls are concrete provider id/name/input/replay with Historical classification.
//! The peer never knows provider syntax, credential secrets or host effects.
//!
//! Contract: domain/run.md, sections 5.2 and 13; programming-model.md, sections 4.4 and 6.3.

use core::mem::size_of;

use alloc::boxed::Box;

use skein_lib::{List, Map, Token};
use smith_domain_run::{self as run, Ask};
use smith_domain_session::{self as session, llm as sllm};

use crate::llm::{self, Block, Decoded, Message, Prompt, Returned, Said, Served};
use crate::translate::{self, DELIVER, FINISH, FIRST, Offered, SUB_AGENT, WAIT};

/// A conversation the run opened, and its session's tickets.
#[derive(Debug)]
pub(crate) struct Peer {
    /// Host-supplied activation used in every name made by this conversation.
    pub(crate) activation: u64,
    /// The run's token for the conversation, which is its session's opener.
    pub(crate) conversation: Token,
    pub(crate) account: Option<u32>,
    pub(crate) retry_at: Option<skein_lib::Time>,
    pub(crate) report: llm::Report,
    pub(crate) model: u32,
    /// The session's token for itself, once it has opened.
    pub(crate) session: Option<Token>,
    offered: Offered,
    /// The asks of the last completion's calls to the tools the run serves,
    /// by ticket, until the session dispatches them; and what they hold.
    asks: Map<u64, Ask>,
    held: u64,
    /// The latest completion has assistant text, excluding refusal and opaque blocks.
    assistant_text: bool,
    /// The next ticket to give.
    next: u64,
}

impl Peer {
    pub(crate) fn new(
        conversation: Token,
        activation: u64,
        account: Option<u32>,
        model: u32,
        offered: Offered,
        limits: &session::Limits,
    ) -> Peer {
        Peer {
            conversation,
            activation,
            account,
            retry_at: None,
            report: llm::Report::none(),
            model,
            session: None,
            offered,
            asks: Map::with_capacity(asks(limits)),
            held: 0,
            assistant_text: false,
            next: FIRST,
        }
    }

    pub(crate) const fn is_main(&self) -> bool {
        self.offered.main
    }

    pub(crate) fn final_choice(&self) -> sllm::ToolChoice {
        assert!(self.offered.main, "only main receives finishing turns");
        if !self.offered.finish {
            return sllm::ToolChoice::None;
        }
        let tickets = if self.offered.deliver { Box::from([FINISH, DELIVER]) } else { Box::from([FINISH]) };
        sllm::ToolChoice::Only(tickets)
    }

    /// Open implicit waiting counts only assistant text; the session also copies refusal text.
    pub(crate) fn yielded_text(&self, text: Box<[u8]>) -> Box<[u8]> {
        if self.offered.main && !self.offered.finish && !self.assistant_text { Box::default() } else { text }
    }

    /// The tickets it holds.
    pub(crate) fn tickets(&self) -> u32 {
        self.asks.len()
    }

    /// The session's view of `completion`: each call to a tool the run serves
    /// becomes a ticket for its ask, while what the asks hold fits `limit`; a
    /// call to one the session was not offered is no call.
    pub(crate) fn completion(&mut self, completion: llm::Completion, limit: u64) -> sllm::Completion {
        let llm::Completion { reasoning_dropped, content, stop, usage } = completion;
        self.assistant_text = false;
        let mut blocks = List::with_capacity(u32::try_from(content.len()).expect("a completion fits in memory"));
        // Every call keeps a classification cell even when decoding is refused.
        // Reserve the complete batch's cells first, so an oversized first call
        // cannot spend the allowance needed by a later TooLarge classification.
        let classification_bytes = u64::try_from(size_of::<Decoded>()).expect("a classification cell fits a u64");
        let mut decoded_bytes = 0_u64;
        for said in &content {
            match said {
                Said::ToolCall { .. } => {
                    decoded_bytes =
                        decoded_bytes.checked_add(classification_bytes).expect("completion cells fit in memory");
                }
                Said::Text { text, .. } => self.assistant_text |= !text.is_empty(),
                Said::Refusal { .. } | Said::Opaque { .. } => {}
            }
        }
        for said in content {
            let block = match said {
                Said::Text { text, replay } => sllm::Block::Text { text, replay },
                Said::Refusal { text, replay } => sllm::Block::Refusal { text, replay },
                Said::Opaque { bytes } => sllm::Block::Opaque { bytes },
                Said::ToolCall { id, name, input, call, replay } => {
                    let total = match call.owned_bytes() {
                        Some(bytes) => match bytes.checked_sub(classification_bytes) {
                            Some(payload) => decoded_bytes.checked_add(payload),
                            None => None,
                        },
                        None => None,
                    };
                    let call = match total {
                        Some(bytes) if bytes <= limit => {
                            decoded_bytes = bytes;
                            self.decoded(call, &name, limit)
                        }
                        Some(_) | None => sllm::Decoded::Invalid {
                            descriptor: self.named_descriptor(&name),
                            problem: sllm::Problem::TooLarge,
                        },
                    };
                    sllm::Block::ToolCall { id, name, input, call, replay }
                }
            };
            blocks.push(block).expect("room for every block");
        }
        sllm::Completion { reasoning_dropped, content: blocks.into_boxed(), stop, usage }
    }

    fn host_offered(&self, name: &[u8], effect: run::HostEffect) -> bool {
        for declaration in &self.offered.host_tools {
            if declaration.name.as_ref() == name {
                return declaration.effect == effect;
            }
        }
        false
    }

    fn host_descriptor(&self, name: &[u8]) -> Token {
        for (index, declaration) in self.offered.host_tools.iter().enumerate() {
            if declaration.name.as_ref() == name {
                return Token::new(
                    FIRST
                        .checked_add(u64::try_from(index).expect("bounded host declarations"))
                        .expect("bounded descriptor"),
                );
            }
        }
        unreachable!("decoded host ask was offered")
    }

    fn named_descriptor(&self, name: &[u8]) -> Option<Token> {
        if name == b"finish" && self.offered.finish {
            return Some(FINISH);
        }
        if name == b"deliver" && self.offered.deliver {
            return Some(DELIVER);
        }
        if name == b"wait" && self.offered.wait {
            return Some(WAIT);
        }
        if name == b"sub_agent" && self.offered.agents {
            return Some(SUB_AGENT);
        }
        for declaration in &self.offered.host_tools {
            if declaration.name.as_ref() == name {
                return Some(self.host_descriptor(name));
            }
        }
        None
    }

    fn decoded(&mut self, call: Decoded, name: &[u8], limit: u64) -> sllm::Decoded {
        let ask = match call {
            Decoded::Builtin { effect } => return sllm::Decoded::Builtin { effect },
            Decoded::Owned { call } => return sllm::Decoded::Owned { call },
            Decoded::Invalid { problem } => {
                return sllm::Decoded::Invalid { descriptor: self.named_descriptor(name), problem };
            }
            Decoded::Served { ask } => ask,
        };
        let offered = match &ask {
            Ask::Wait => self.offered.wait,
            Ask::Host { tool, effect, .. } => self.host_offered(tool, *effect),
            Ask::Finish { .. } => self.offered.finish,
            Ask::Deliver { .. } => self.offered.deliver,
            Ask::SubAgent { .. } => self.offered.agents,
        };
        if !offered {
            return sllm::Decoded::Invalid { descriptor: None, problem: sllm::Problem::UnknownTool };
        }
        let held = match ask_cost(&ask) {
            Some(cost) => self.held.checked_add(cost),
            None => None,
        };
        let held = match held {
            Some(held) if held <= limit && self.asks.len() < self.asks.capacity() => held,
            Some(_) | None => {
                return sllm::Decoded::Invalid {
                    descriptor: self.named_descriptor(name),
                    problem: sllm::Problem::TooLarge,
                };
            }
        };
        let source = match &ask {
            Ask::Host { .. } => session::ToolSource::Host,
            Ask::Wait | Ask::Finish { .. } | Ask::Deliver { .. } | Ask::SubAgent { .. } => session::ToolSource::Run,
        };
        let descriptor = match &ask {
            Ask::Finish { .. } => FINISH,
            Ask::Deliver { .. } => DELIVER,
            Ask::SubAgent { .. } => SUB_AGENT,
            Ask::Wait => WAIT,
            Ask::Host { tool, .. } => self.host_descriptor(tool),
        };
        let effect = translate::effect(&ask);
        let ticket = self.ticket();
        let fresh = self.asks.insert(ticket, ask).expect("checked for room above");
        assert!(fresh.is_none(), "tickets are not reused");
        self.held = held;
        sllm::Decoded::Delegated { source, ticket: Token::new(ticket), descriptor, effect }
    }

    /// The ask the session dispatches as `ticket`.
    pub(crate) fn take(&mut self, ticket: Token) -> Ask {
        self.asks.remove(&ticket.raw()).expect("a session dispatches a call once, by the ticket it was given")
    }

    /// The session goes on without the calls it has not dispatched: it
    /// yielded, or called the LLM again. How many tickets went.
    pub(crate) fn forget_asks(&mut self, limits: &session::Limits) -> u32 {
        let forgotten = self.asks.len();
        self.asks = Map::with_capacity(asks(limits));
        self.held = 0;
        forgotten
    }

    /// The protocol layer's view of the session's `prompt`: every ticket in it
    /// resolved, the answers copied.
    pub(crate) fn prompt(&self, prompt: sllm::Prompt) -> Prompt {
        let sllm::Prompt {
            purpose,
            endpoint,
            affinity,
            protocol_allowance: _,
            model,
            effort,
            system,
            tools,
            namespace,
            delegated,
            choice,
            messages,
            max_tokens,
        } = prompt;
        let mut served = List::with_capacity(u32::try_from(delegated.len()).expect("bounded declaration inventory"));
        for descriptor in &delegated {
            let tool = self.descriptor(descriptor.ticket);
            served.push(tool).expect("room for every descriptor");
        }
        let choice = self.choice(choice);
        let mut resolved = List::with_capacity(u32::try_from(messages.len()).expect("a transcript fits its limit"));
        for sllm::Message { role, content } in messages {
            let mut blocks = List::with_capacity(u32::try_from(content.len()).expect("a message fits in memory"));
            for block in content {
                blocks.push(Self::block(block)).expect("room for every block");
            }
            resolved.push(Message { role, content: blocks.into_boxed() }).expect("room for every message");
        }
        Prompt {
            purpose,
            asker: if self.is_main() { llm::Asker::Main } else { llm::Asker::Child },
            endpoint,
            affinity,
            model,
            effort,
            system,
            tools,
            namespace,
            served: served.into_boxed(),
            choice,
            messages: resolved.into_boxed(),
            max_tokens,
        }
    }

    fn descriptor(&self, ticket: Token) -> Served {
        match ticket {
            WAIT => Served::Wait,
            DELIVER => Served::Deliver,
            FINISH => Served::Finish,
            SUB_AGENT => Served::SubAgent,
            ticket => {
                let index = usize::try_from(ticket.raw().checked_sub(FIRST).expect("host descriptor range"))
                    .expect("bounded host descriptor");
                Served::Host(self.offered.host_tools.get(index).expect("descriptor names admitted declaration").clone())
            }
        }
    }

    /// Bytes of opaque host declarations copied into the protocol prompt.
    /// They are absent from the session's ticket-only prompt count.
    pub(crate) fn declaration_bytes(&self) -> Option<u64> {
        let mut bytes = 0_u64;
        for tool in &self.offered.host_tools {
            bytes = bytes
                .checked_add(u64::try_from(tool.name.len()).ok()?)?
                .checked_add(u64::try_from(tool.description.len()).ok()?)?
                .checked_add(u64::try_from(tool.schema.len()).ok()?)?;
        }
        Some(bytes)
    }

    fn choice(&self, choice: sllm::ToolChoice) -> llm::ToolChoice {
        match choice {
            sllm::ToolChoice::Auto => llm::ToolChoice::Auto,
            sllm::ToolChoice::None => llm::ToolChoice::None,
            sllm::ToolChoice::Only(tickets) => {
                let mut selected = List::with_capacity(u32::try_from(tickets.len()).expect("bounded choice"));
                for ticket in tickets {
                    selected.push(self.descriptor(ticket)).expect("one choice cell per ticket");
                }
                llm::ToolChoice::Only(selected.into_boxed())
            }
        }
    }

    fn block(block: sllm::Block) -> Block {
        match block {
            sllm::Block::Text { text, replay } => Block::Text { text, replay },
            sllm::Block::Refusal { text, replay } => Block::Refusal { text, replay },
            sllm::Block::Opaque { bytes } => Block::Opaque { bytes },
            sllm::Block::ToolCall { id, name, input, call: _, replay } => Block::ToolCall { id, name, input, replay },
            sllm::Block::ToolResult { id, result } => {
                let result = match result {
                    sllm::Returned::Owned { outcome } => Returned::Owned { outcome },
                    sllm::Returned::Invalid { problem } => Returned::Invalid { problem },
                    sllm::Returned::NotRun { why } => Returned::NotRun {
                        why: match why {
                            sllm::NotRun::Stopped => llm::NotRun::Stopped,
                            sllm::NotRun::AfterFailedWrite { call } => llm::NotRun::AfterFailedWrite { call },
                            sllm::NotRun::OutsideChoice { choice } => llm::NotRun::OutsideChoice { choice },
                        },
                    },
                    sllm::Returned::Text { text, error, replay } => Returned::Text { text, error, replay },
                    sllm::Returned::Withdrawn => Returned::Withdrawn,
                };
                Block::ToolResult { id, result }
            }
        }
    }

    pub(crate) fn retained_choice(&self, choice: sllm::ToolChoice) -> Option<Box<[Box<[u8]>]>> {
        match choice {
            sllm::ToolChoice::Auto => unreachable!("automatic choices reject no call"),
            sllm::ToolChoice::None => None,
            sllm::ToolChoice::Only(tickets) => {
                let mut names = List::with_capacity(u32::try_from(tickets.len()).expect("bounded choice"));
                for ticket in tickets {
                    let name = match self.descriptor(ticket) {
                        Served::Wait => Box::from(b"wait".as_slice()),
                        Served::Deliver => Box::from(b"deliver".as_slice()),
                        Served::Finish => Box::from(b"finish".as_slice()),
                        Served::SubAgent => Box::from(b"sub_agent".as_slice()),
                        Served::Host(tool) => tool.name,
                    };
                    names.push(name).expect("one name per retained ticket");
                }
                Some(names.into_boxed())
            }
        }
    }

    fn ticket(&mut self) -> u64 {
        let ticket = self.next;
        self.next = ticket.checked_add(1).expect("a session makes fewer calls than a u64 counts");
        ticket
    }
}

/// The most asks a peer holds: as many as fit the session's byte limit at
/// their fixed size.
pub(crate) fn asks(limits: &session::Limits) -> u32 {
    per(limits.history_bytes, size_of::<Ask>()).min(limits.completion_blocks)
}

fn per(bytes: u64, size: usize) -> u32 {
    let size = u64::try_from(size).expect("a size fits in a u64").max(1);
    u32::try_from(bytes.checked_div(size).unwrap_or(0)).unwrap_or(u32::MAX)
}

/// What an ask holds: its fixed size, and each part held in a box at its
/// fixed size plus its payload.
pub(crate) fn ask_cost(ask: &Ask) -> Option<u64> {
    let payload = match ask {
        Ask::Wait => 0,
        Ask::Host { tool, input, .. } => size(tool.len())?.checked_add(size(input.bytes().len())?)?,
        Ask::Deliver { change } => change.owned_bytes()?,
        Ask::Finish { outcome } => run::outcome::owned_bytes(outcome)?,
        Ask::SubAgent { brief, families: _, llm, share: _ } => {
            let llm = match llm {
                Some(llm) => len(llm)?,
                None => 0,
            };
            len(brief)?.checked_add(llm)?
        }
    };
    size(size_of::<Ask>())?.checked_add(payload)
}

fn len(bytes: &[u8]) -> Option<u64> {
    size(bytes.len())
}

fn size(size: usize) -> Option<u64> {
    u64::try_from(size).ok()
}
