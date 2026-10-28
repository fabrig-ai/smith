//! The application world's actual prepared shared Client and scripted byte peer.
//! Application descriptors/decoding stay here; transport and provider grammar
//! belong entirely to Skein. Existing World owns clocks, root and checkout work.
//! This module retains adapter context until the actual terminal and each physical
//! Client until actual lower Closed. Composition routes active callbacks separately
//! from retired outside observations; it never reads private domain state. Prepare,
//! start, tick, cancel, close and settle are distinct entrances under caller clocks
//! (scratch/client.md, sections 1, 4, 5 and 6; programming-model.md, sections 5.2 and 9).

use std::collections::BTreeMap;

use skein_lib::{Time, Token, Wall};
use skein_llm::{self as shared, client};
use skein_llm_world::fake::Exchange;
use smith_domain::{Event, llm, tools};
use smith_protocol_llm::{self as adapter, Context, Limits, Receiving, ResolvedCall, ToolKind, ToolSchema};

/// Caller-supplied endpoint and credential data for the wire fixture.
/// Caller owns this finite configuration; admission and terminals belong to Wire.
/// Contract: scratch/client.md, sections 1 and 6.
#[expect(missing_debug_implementations, reason = "fixture owns caller credential bytes")]
pub struct Configuration {
    /// Whole shared endpoint configuration, supplied as neutral caller data.
    /// Contract: scratch/client.md, section 1.
    pub endpoint: shared::Endpoint,
    /// Actual grant supplied to one Client and separately to its independent peer.
    /// Contract: scratch/client.md, sections 1 and 6.
    pub credential: shared::Credential,
    /// Independent outside expectation for a native error result's text prefix.
    /// Contract: scratch/client.md, section 3; testing-strategy.md, section 7.
    pub error_prefix: Box<[u8]>,
    /// Independent outside expectation for a native error result's parsed flag.
    /// Contract: scratch/client.md, section 3; testing-strategy.md, section 7.
    pub error_flag: bool,
    /// Handwritten outside expectation for this fixture's continued arguments.
    /// Incoming delta bytes remain exact at the root. A native embedded-object
    /// continuation may serialize object whitespace while preserving every field.
    /// Contract: scratch/client.md, section 3; testing-strategy.md, section 7.
    pub continuation_arguments: Box<[u8]>,
}

/// Native configurations are shared kit data, with no Smith dialect branching.
/// Returns two caller fixtures; it starts no effect and carries outside literal
/// expectations. Contract: scratch/client.md, section 1; testing-strategy.md, section 7.
#[must_use]
pub fn configurations() -> Box<[Configuration]> {
    Box::new([
        Configuration {
            endpoint: shared::Endpoint::codex(),
            credential: shared::Credential {
                value: b"fixture-token".as_slice().into(),
                account_id: b"fixture-account".as_slice().into(), scheme: skein_llm::Scheme::Bearer,},
            error_prefix: b"Error: ".as_slice().into(),
            error_flag: false,
            continuation_arguments:
                br#"{ "opaque" : {"future":[1,true,null]}, "extra":"unchanged" }"#.as_slice().into(),
        },
        Configuration {
            endpoint: shared::Endpoint::anthropic(),
            credential: shared::Credential::anthropic(b"fixture-token".as_slice().into()),
            error_prefix: Box::new([]),
            error_flag: true,
            continuation_arguments: br#"{"opaque":{"future":[1,true,null]},"extra":"unchanged"}"#.as_slice().into(),
        },
    ])
}

/// Finite actual lower chronology, without retaining another completion body.
/// Shared Client emits these observations; at most four belong to one Wire.
/// Contract: scratch/client.md, sections 4 and 5.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Observed {
    /// Actual completed callback. Contract: scratch/client.md, section 4.
    Completed(
        /// Opaque root request owner supplied at preparation and echoed by the
        /// actual Client's one terminal. Contract: scratch/client.md, sections 1 and 4.
        Token,
    ),

    /// Actual failed callback. Contract: scratch/client.md, section 4.
    Failed(
        /// Opaque root request owner supplied at preparation and echoed by the
        /// actual Client's one terminal. Contract: scratch/client.md, sections 1 and 4.
        Token,
    ),

    /// Actual cancelled callback after settlement. Contract: scratch/client.md, section 5.
    Cancelled(
        /// Opaque root request owner supplied at preparation and echoed after
        /// actual lower settlement. Contract: scratch/client.md, sections 1 and 5.
        Token,
    ),

    /// Terminal won; transport drainage permits reuse. Contract: scratch/client.md, section 5.
    Reusable,

    /// Lower physical Close requested. Contract: scratch/client.md, section 5.
    Close,

    /// Actual lower Closed received; physical owner may retire. Contract: scratch/client.md, section 5.
    Closed,
}

/// One actual physical binding's outside lifecycle, retained after retirement.
/// It keeps receiving metadata and content-free lower observations, not a second
/// completion or a credential. Contract: scratch/client.md, sections 1, 5 and 6;
/// domain/run.md, sections 3 and 14; testing-strategy.md, sections 2.3 and 6.
#[derive(Debug)]
pub struct Binding {
    /// Root-issued logical callback; later physical calls may reuse it.
    /// Contract: scratch/client.md, sections 1 and 5.
    pub owner: Token,

    /// Exact advertised root receiving contract used before actual preparation.
    /// Contract: scratch/client.md, sections 1 and 6.
    pub receiving: Receiving,
    /// Actual SDK terminal usage, before application translation.
    /// Contract: scratch/client.md, section 4; domain/run.md, section 9.
    pub accepted_usage: Option<shared::Usage>,

    /// Shared root/Client/peer iteration clock when this physical call started.
    /// Contract: scratch/client.md, section 1; programming-model.md, section 9.
    pub started: (Time, Wall),

    /// Other actual physical bindings still retained when this call started.
    /// This outside count includes older calls whose root terminal already won.
    /// Contract: scratch/client.md, section 5.
    pub retained_at_start: u32,

    /// One terminal and bounded Reusable/Close/Closed chronology with clocks.
    /// Contract: scratch/client.md, sections 4 and 5.
    pub observed: Vec<(Time, Wall, Observed)>,

    /// Retirement clock, present only after actual lower Closed was observed.
    /// Contract: scratch/client.md, section 5; programming-model.md, section 5.2.
    pub retired: Option<(Time, Wall)>,

    wire: Option<Wire>,
    seen: usize,
}

/// Application-owned routing of actual Clients in the existing root world.
/// Generic transport, peer progress and all clocks remain in shared Exchange.
/// Active logical callbacks are separate from retained physical bindings.
/// Contract: scratch/client.md, sections 1 and 5; domain/run.md, section 14.
pub(crate) struct Composition {
    configuration: Configuration,
    limits: Limits,
    scripts: Box<[skein_fake_llm_domain::api::Script]>,
    active: BTreeMap<Token, usize>,
    bindings: Vec<Binding>,
}

impl core::fmt::Debug for Composition {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("Composition")
            .field("active", &self.active)
            .field("bindings", &self.bindings)
            .finish_non_exhaustive()
    }
}

/// Owned observations from one bounded shared-world progress round.
/// Contract: scratch/client.md, sections 4 and 5; testing-strategy.md, section 6.
pub(crate) struct Progress {
    /// Actual protocol/close progress; prevents root timers skipping queued bytes.
    /// Contract: scratch/client.md, section 5; programming-model.md, section 9.
    pub(crate) immediate: bool,
    /// Actual adapter-translated terminals, each consuming its active callback.
    /// Contract: scratch/client.md, section 4.
    pub(crate) terminals: Vec<(Token, Event)>,
    /// Actual byte-peer decoded queries, never reconstructed from a root prompt.
    /// Contract: scratch/client.md, section 3; testing-strategy.md, section 2.3.
    pub(crate) queries: Vec<skein_fake_llm_domain::api::Query>,
}

impl Composition {
    /// Caller data for an opt-in actual-wire fixture, with 256 physical calls
    /// as its asserted finite story ceiling. Contract: scratch/client.md, sections 1 and 6.
    pub(crate) fn new(
        configuration: Configuration,
        limits: Limits,
        scripts: Box<[skein_fake_llm_domain::api::Script]>,
    ) -> Self {
        Self { configuration, limits, scripts, active: BTreeMap::new(), bindings: Vec::new() }
    }

    /// The actual root Complete supplies every receiving field and the owning
    /// prompt. No wire starts before adapter admission. Contract: scratch/client.md, sections 1 and 6.
    pub(crate) fn start(&mut self, owner: Token, prompt: llm::Prompt, receiving: Receiving, now: Time, wall: Wall) {
        assert!(self.bindings.len() < 256, "actual wire fixture has a finite physical-call ceiling");
        let retained_at_start = u32::try_from(self.bindings.iter().filter(|binding| binding.wire.is_some()).count())
            .expect("bounded physical bindings");
        let mut wire = Wire::prepare(owner, prompt, receiving, &self.configuration, &self.limits, self.scripts.clone())
            .expect("caller supplied compatible actual root/Client receiving limits");
        wire.peer.at(now, wall);
        let index = self.bindings.len();
        assert!(self.active.insert(owner, index).is_none(), "one active actual Client per logical callback");
        assert!(wire.start().is_empty(), "admitted actual Start waits for lower progress before its terminal");
        self.bindings.push(Binding {
            owner,
            receiving,
            accepted_usage: None,
            started: (now, wall),
            retained_at_start,
            observed: Vec::new(),
            retired: None,
            wire: Some(wire),
            seen: 0,
        });
    }

    /// Actual root cancellation addresses only its current physical Client.
    /// The terminal remains owed until lower settlement. Contract: scratch/client.md, 5.
    pub(crate) fn cancel(&mut self, owner: Token, now: Time, wall: Wall) {
        if let Some(index) = self.active.get(&owner) {
            let wire = self.bindings[*index].wire.as_mut().expect("an active physical binding is retained");
            wire.peer.at(now, wall);
            assert!(wire.cancel().is_empty(), "actual Cancel waits for lower Closed, never invents a terminal");
        }
    }

    /// Progresses each retained physical binding once under the root's clocks.
    /// Completed connections are held through Reusable, explicit Close and a
    /// later lower Closed; the next call may coexist with that old close right.
    /// The fixture deliberately holds won Close until a new call or root answer;
    /// this is a selected lower settlement schedule, not application policy.
    /// Contract: scratch/client.md, sections 4 and 5; programming-model.md, sections 5.2 and 9.
    pub(crate) fn advance(&mut self, now: Time, wall: Wall, ending: bool) -> Progress {
        let mut progress = Progress { immediate: false, terminals: Vec::new(), queries: Vec::new() };
        for (index, binding) in self.bindings.iter_mut().enumerate() {
            let Some(wire) = binding.wire.as_mut() else { continue };
            wire.peer.at(now, wall);
            let returned = if wire.observed.contains(&Observed::Close) {
                if ending || !self.active.is_empty() {
                    progress.immediate = true;
                    wire.settle()
                } else {
                    Vec::new()
                }
            } else {
                let (immediate, returned) = wire.tick();
                progress.immediate |= immediate;
                if wire.observed.contains(&Observed::Reusable) {
                    assert!(wire.close().is_empty(), "closing the won terminal cannot emit another one");
                    progress.immediate = true;
                }
                returned
            };
            binding.accepted_usage = wire.accepted_usage;
            progress.queries.append(&mut wire.peer.queries);
            for event in returned {
                assert_eq!(
                    self.active.remove(&binding.owner),
                    Some(index),
                    "terminal consumes its exact physical binding"
                );
                progress.terminals.push((binding.owner, event));
            }
            binding.observed.extend(wire.observed[binding.seen..].iter().map(|observed| (now, wall, *observed)));
            binding.seen = wire.observed.len();
            if wire.observed.contains(&Observed::Closed) {
                assert!(
                    self.active.get(&binding.owner) != Some(&index),
                    "retiring a physical binding retains any newer logical owner"
                );
                binding.retired = Some((now, wall));
                binding.wire = None;
            }
        }
        progress
    }

    /// Retained outside observations after actual lower progress; no domain
    /// internals determine this chronology. Contract: scratch/client.md, section 5.
    pub(crate) fn bindings(&self) -> &[Binding] {
        &self.bindings
    }

    /// No active callback or retained Client remains. Contract: scratch/client.md, section 5.
    pub(crate) fn is_settled(&self) -> bool {
        self.active.is_empty() && self.bindings.iter().all(|binding| binding.wire.is_none())
    }
}

/// Owns the actual prepared machine and retained adapter context through settlement.
/// Caller supplies receiving caps and clocks; there is at most one root terminal
/// and four lifecycle observations. Contract: scratch/client.md, sections 1, 4, 5 and 6.
pub struct Wire {
    /// The one actual Client, lower HTTP/SSE peer and shared script domain.
    /// Contract: scratch/client.md, sections 1 and 5.
    pub peer: Exchange,
    /// At most one terminal plus Reusable/Close/Closed, without diagnostic bodies.
    /// Contract: scratch/client.md, sections 4 and 5.
    pub observed: Vec<Observed>,
    /// Exact SDK usage copied before adapter translation; no duplicate body.
    /// Contract: scratch/client.md, section 4; domain/run.md, section 9.
    pub accepted_usage: Option<shared::Usage>,
    context: Option<Context>,
}

impl core::fmt::Debug for Wire {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("Wire")
            .field("waiting", &self.peer.machine.waiting())
            .field("observed", &self.observed)
            .field("accepted_usage", &self.accepted_usage)
            .field("live_context", &self.context.is_some())
            .finish()
    }
}

impl Wire {
    /// The root's actual Complete request supplies owner/prompt/receiving metadata.
    /// Preparation is effect free; start is a separate Client entrance.
    /// Success owns one prepared Client/context; failure starts nothing.
    /// Contract: scratch/client.md, sections 1 and 6.
    ///
    /// # Errors
    /// Returns the adapter's admission error when the complete prompt, application
    /// descriptors or receiving contract cannot fit the supplied limits.
    pub fn prepare(
        owner: Token,
        prompt: llm::Prompt,
        receiving: Receiving,
        configuration: &Configuration,
        limits: &Limits,
        scripts: Box<[skein_fake_llm_domain::api::Script]>,
    ) -> Result<Self, adapter::Error> {
        let endpoint_name = prompt.endpoint;
        let application = schemas(&prompt);
        let endpoint = configuration.endpoint.clone();
        let peer_endpoint = endpoint.clone();
        let credential = shared::Credential {
            value: configuration.credential.value.clone(),
            account_id: configuration.credential.account_id.clone(), scheme: skein_llm::Scheme::Bearer,};
        let peer_credential = shared::Credential {
            value: credential.value.clone(),
            account_id: credential.account_id.clone(), scheme: skein_llm::Scheme::Bearer,};
        let adapter::Prepared { client, context } = adapter::prepare(
            adapter::Input {
                owner,
                prompt,
                endpoint_name,
                endpoint,
                credential,
                application,
                results: Box::new([]),
                receiving,
            },
            limits,
        )?;
        let peer = Exchange::prepared(client, peer_endpoint, Some(peer_credential), limits.client, scripts);
        Ok(Self { peer, context: Some(context), observed: Vec::new(), accepted_usage: None })
    }

    /// Caller starts the admitted actual Client once under its installed clocks.
    /// Returns only actual translated events; no callback is fabricated.
    /// Contract: scratch/client.md, sections 1 and 4.
    pub fn start(&mut self) -> Vec<Event> {
        self.peer.start();
        self.take()
    }

    /// One shared-world progress step. The root world may interleave timers,
    /// actual cancellation requests and other delegated effects between steps.
    /// Returns immediate progress and at most one actual root terminal, preserving
    /// its complete receiving contract. Contract: scratch/client.md, sections 4 and 5.
    pub fn tick(&mut self) -> (bool, Vec<Event>) {
        let progress = self.peer.tick(true);
        (progress, self.take())
    }

    /// Root requests cancellation; the callback remains owed until actual lower
    /// settlement. Returns only actual events. Contract: scratch/client.md, section 5.
    pub fn cancel(&mut self) -> Vec<Event> {
        self.peer.request(client::Request::Cancel);
        self.take()
    }

    /// Physical owner requests explicit Close after Reusable; no second root
    /// terminal is invented. Contract: scratch/client.md, section 5.
    pub fn close(&mut self) -> Vec<Event> {
        self.peer.request(client::Request::Close);
        self.take()
    }

    /// Called only after the world's observed lower Close has actually settled.
    /// Feeds shared lower Closed and returns any actual owed cancellation terminal.
    /// Contract: scratch/client.md, section 5; programming-model.md, section 5.2.
    pub fn settle(&mut self) -> Vec<Event> {
        self.peer.settle();
        self.take()
    }

    fn take(&mut self) -> Vec<Event> {
        let mut returned = Vec::new();
        for event in core::mem::take(&mut self.peer.seen) {
            match event {
                client::Event::Completed { owner, completion } => {
                    self.observed.push(Observed::Completed(owner));
                    self.accepted_usage = Some(completion.usage);
                    let context = self.context.take().expect("one actual Client terminal owns the context");
                    let decoded = resolutions(&context, &completion);
                    returned.push(
                        adapter::completion(context, owner, completion, decoded)
                            .expect("actual admitted Client completion and explicit application codec fit receiving"),
                    );
                }
                client::Event::Failed { owner, failure, evidence, report, detail } => {
                    self.observed.push(Observed::Failed(owner));
                    let context = self.context.take().expect("one actual Client terminal owns the context");
                    returned.push(
                        adapter::failed(context, owner, failure, evidence, detail, report).expect(
                            "actual admitted failure retains owner, class, evidence and full bounded diagnostic",
                        ),
                    );
                }
                client::Event::Cancelled { owner } => {
                    self.observed.push(Observed::Cancelled(owner));
                    let context = self.context.take().expect("one actual Client terminal owns the context");
                    returned.push(adapter::cancelled(context, owner).expect("actual cancellation callback owner"));
                }
                client::Event::Reusable => self.observed.push(Observed::Reusable),
                client::Event::Close => self.observed.push(Observed::Close),
                client::Event::Closed => self.observed.push(Observed::Closed),
                client::Event::Delta { .. } | client::Event::Block { .. } => {}
            }
        }
        assert!(self.observed.len() <= 4, "one actual terminal and finite lower settlement chronology");
        assert!(returned.len() <= 1, "one root terminal per actual wire call");
        returned
    }
}

fn resolutions(context: &Context, completion: &shared::Completion) -> Box<[ResolvedCall]> {
    completion
        .content
        .iter()
        .enumerate()
        .filter_map(|(position, block)| match block {
            shared::Block::ToolCall { name, arguments, .. } => {
                if context.served().iter().any(|declaration| match declaration {
                    llm::Served::Host(tool) => tool.name == *name,
                    llm::Served::Finish | llm::Served::Deliver | llm::Served::SubAgent | llm::Served::Wait => false,
                }) {
                    return None;
                }
                Some(ResolvedCall {
                    position: u32::try_from(position).expect("bounded actual assistant position"),
                    name: name.clone(),
                    input: arguments.clone(),
                    call: crate::translate::decode(name, arguments, context.grants(), context.served(), context.asker()),
                })
            }
            shared::Block::Text { .. }
            | shared::Block::Refusal { .. }
            | shared::Block::Reasoning { .. }
            | shared::Block::ToolResult { .. } => None,
        })
        .collect()
}

/// Explicit whole schemas for this world's finite application fixture language.
/// These are caller data, not provider grammar or production schema policy.
/// Caller gets one descriptor per offered fixture capability; root/adapter
/// admission bounds the whole inventory. Contract: scratch/client.md, sections 1 and 3.
#[must_use]
pub fn schemas(prompt: &llm::Prompt) -> Box<[ToolSchema]> {
    let mut descriptors = Vec::new();
    if prompt.tools.inspect {
        descriptors.extend([
            descriptor(ToolKind::Owned(tools::Tool::Read), b"read", br#"{"type":"object","properties":{"path":{"type":"string"}},"required":["path"],"additionalProperties":false}"#),
            descriptor(ToolKind::Owned(tools::Tool::List), b"list", br#"{"type":"object","properties":{"path":{"type":"string"}},"required":["path"],"additionalProperties":false}"#),
            descriptor(ToolKind::Owned(tools::Tool::Search), b"search", br#"{"type":"object","properties":{"path":{"type":"string"},"pattern":{"type":"string"}},"required":["path","pattern"],"additionalProperties":false}"#),
        ]);
    }
    if prompt.tools.modify {
        descriptors.extend([
            descriptor(ToolKind::Owned(tools::Tool::Remove), b"remove", br#"{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}"#),
            descriptor(ToolKind::Owned(tools::Tool::Write), b"write", br#"{"type":"object","properties":{"path":{"type":"string"},"content":{"type":"string"}},"required":["path","content"],"additionalProperties":false}"#),
            descriptor(ToolKind::Owned(tools::Tool::Edit), b"edit", br#"{"type":"object","properties":{"path":{"type":"string"},"edits":{"type":"array","minItems":1,"items":{"type":"object","properties":{"old":{"type":"string"},"new":{"type":"string"},"all":{"type":"boolean"}},"required":["old","new"]}}},"required":["path","edits"],"additionalProperties":false}"#),
        ]);
    }
    if prompt.tools.shell {
        descriptors.push(descriptor(ToolKind::Owned(tools::Tool::Shell), b"shell", br#"{"type":"object","properties":{"command":{"type":"string"}},"required":["command"],"additionalProperties":false}"#));
    }
    for served in &prompt.served {
        match served {
            llm::Served::Host(_) => {}
            llm::Served::Wait => descriptors.push(descriptor(ToolKind::Wait, b"wait", br#"{"type":"object","properties":{},"additionalProperties":false}"#)),
            llm::Served::Deliver => descriptors.push(descriptor(ToolKind::Deliver, b"deliver", br#"{"type":"object","properties":{"ticket":{"type":"string"}},"required":["ticket"],"additionalProperties":false}"#)),
            llm::Served::Finish => descriptors.push(descriptor(ToolKind::Finish, b"finish", br#"{"type":"object","properties":{"title":{"type":"string"},"body":{"type":"string"},"report":{"type":"string"},"source":{"type":"string"},"failure":{"type":"string"},"cause":{"type":"string"},"verdict":{"type":"string"},"children":{"type":"array","items":{"type":"object","properties":{"kind":{"type":"string"},"path":{"type":"string"}},"required":["kind","path"],"additionalProperties":false}}},"anyOf":[{"required":["title","body"]},{"required":["report"]},{"required":["failure"]},{"required":["verdict","body"]}],"additionalProperties":false}"#)),
            llm::Served::SubAgent => descriptors.push(descriptor(ToolKind::SubAgent, b"sub_agent", br#"{"type":"object","properties":{"brief":{"type":"string"},"tools":{"type":"array","items":{"type":"string","enum":["inspect","modify","shell"]}},"llm":{"type":"string"}},"required":["brief","tools"],"additionalProperties":false}"#)),
        }
    }
    descriptors.into()
}

fn descriptor(kind: ToolKind, name: &[u8], schema: &[u8]) -> ToolSchema {
    ToolSchema {
        kind,
        name: name.into(),
        description: b"Finite application world tool.".as_slice().into(),
        schema: schema.into(),
    }
}
