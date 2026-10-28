# Limits

Provisional, 2026-10-09, revised 2026-10-20. How an agent's limits are
made:

- the quantities a deployment declares;
- how every limit below them is derived, layer by layer;
- the relationships checked at startup, each by name;
- memory, as admission to a shared LLM pool against a real target;
- what happens to one call at a limit, and the deadlines that measure
  progress;
- how a limit that fires is reported.

The derivation's skein half is skein's `llm.md`. The values a product
ships are its presets, in `docs/design/shell.md`. This document holds
the rules and the arithmetic's shape, not the values.

## 1. In one page

- **Declared, never set by hand.** A deployment declares a few
  quantities: per model, its window, its output, its largest reasoning
  item and its progress deadlines; per deployment, its concurrency, its
  tool payload, its calls per response, what a tool's output may hold,
  the jobs a session may hold, its LLM pool and its memory (section 2).
  No byte limit is configuration. A test may override a derived limit;
  nothing else may.
- **A profile is a named bundle** of declared values and policy values.
  It is step code in the service crates (README.md, section 5), so a
  profile is tested where it is made, and its values are the product's
  presets.
- **Every limit is derived, layer by layer,** with checked arithmetic:
  skein-llm's `Limits::derive` gives the LLM path's, and smith derives
  its session's, transcript's, rendering's, machine's and channel's from
  the same declarations and from the layers below (section 3). An
  overflow refuses the configuration.
- **The window bounds the material, not the reverse.** A byte counts as
  at most one token. What a window holds at its opening, and what a
  compaction carries, is bounded from the model's window, so every
  window can open and close within it (3.9). The session's check
  estimates appended bytes at a measured ratio; a guard holds each
  prompt sent within the line below which a compaction always fits
  (3.9).
- **Every relationship is checked at startup, by name** (section 4). A
  violation refuses the configuration and names the relationship and the
  values that broke it. What startup can find is never found in the
  middle of a run.
- **Memory is a real target** (section 5). LLM calls are admitted to a
  shared pool of declared size and wait while it is full, so the worst
  case is the pool's size, not a product of independent maxima. The sum
  of every worst case must fit the declared memory, which is real
  memory.
- **A limit meets one call, not the conversation** (section 6). An
  oversized call is that call's problem, told to the model as one it can
  fix. Oversized rendered output is cut with a marker. A call that finds
  the pool full waits.
- **Deadlines measure progress** (section 7): connecting, the handshake,
  the response's head and the gaps between its events. The whole call is
  bounded only by the session's remaining time.
- **A limit that fires says which** (section 8): `Limit { which, bound }`
  in the failure, in the answer, in the event stream and in the
  operator's words.

## 2. Declared quantities

### 2.1 Per model

Declared for each model an endpoint serves, in the agent's configuration
(agent.md, section 4). A provider preset supplies known models' values;
settings override them (`docs/design/shell.md`).

| Quantity | Unit | What it is |
|---|---|---|
| `window` | tokens | the usable input window: the most one prompt may hold, with a completion of `output` beside it |
| `output` | tokens | the most one completion may produce, thinking included: sent where the dialect takes it, what a completion's budget reserves (domain/run.md, section 9), and what the window above its threshold holds for the answer a compaction carries (3.9) |
| `reasoning_item` | bytes | the largest opaque reasoning item the model returns for replay |
| `head` | duration | from the request sent to the response's first byte |
| `idle` | duration | the largest gap between two events of a streaming response |

- **The charter carries the effective values.** A host puts a model's
  `window` and `output` in each charter's model entry, at most what the
  configuration declares for it (charter.md, section 2). The agent
  derives its limits from the configuration's, the largest any charter
  may name; the session enforces the charter's.
- **A window is a prompt's, never its output's.** No request's output
  takes room in `window`: the session's check bounds the next prompt,
  and a compaction's summary is its output (domain/session.md, 8.1 and
  8.2). Where a provider counts a request's output within its context, a
  model's `window` is declared as that context less `output`.
- **An oversized reasoning item fails by default.** A model's
  declaration may opt into dropping it instead: the completion is kept,
  the item is left out of later requests, and the event stream says so
  (events.md, section 3.9).

### 2.2 Per deployment

| Quantity | Unit | What it is |
|---|---|---|
| `conversations` | count | the most conversations open at once across a run's tree: main, its sub-agents and compactions; so the most LLM calls in flight |
| `tool_payload` | bytes | the most a tool exchanges in one piece: a `read`'s largest window, a `write`'s content, the arguments of one call |
| `file_bytes` | bytes | the largest file the tools hold whole: a file `read` pages through in windows, and one `edit` loads and stores. It is at least `tool_payload`, so a file can grow past one piece: written once, then extended by edits |
| `calls_per_response` | count | the most tool calls one completion may make, and so the most result blocks the next message holds |
| `jobs` | count | the most jobs one session may hold at once: a job counts from the answer that names it `running` until an answer reports its end, or its session closes (domain/tools.md, section 5) |
| render caps | bytes, count | what a tool's output may hold (below) |
| `llm_pool` | bytes | the memory every LLM call in flight shares, sending and receiving (section 5) |
| `memory` | bytes | the process's real memory: every worst case must fit within it (section 5) |

The render caps, each the default or the ceiling a tool states to the
model (domain/tools.md, section 4):

| Cap | What it bounds |
|---|---|
| `read_window` | a `read`'s window when the call asks for none; the ceiling is `tool_payload` |
| `shell_output` | a command's kept output when the call asks for none |
| `shell_head`, `shell_tail` | the ceiling on a command's kept output, and how it splits between head and tail; a check's output keeps the same tail |
| `search_hits`, `search_bytes` | a search's matches and their bytes |
| `list_entries` | a listing's entries |
| `guide` | a guide kept whole in the system text before it is cut (domain/run.md, 3.3) |

### 2.3 Policy values

Values of a profile that bound no bytes, checked like any. Each has the
name `smith check` and the event stream's header print it under
(section 9); the product's values are `docs/design/shell.md`'s, section
6.5.

| Name | Unit | What it is |
|---|---|---|
| `max_turns`, `max_spend`, `max_time`, `max_waiting` | count; the host's unit; duration; duration | the most a charter's budget and waiting time may ask for (domain/run.md, section 9) |
| `sections`, `host_tools`, `verdicts`, `items`, `fields` | count | the most of each a charter may hold, at most the charter schema's (charter.md, section 6) |
| `inbox` | count | the messages a run holds unread (domain/run.md, section 6) |
| `unacknowledged` | count | the most turns a start's window may ask to hold unacknowledged (channel.md, section 7) |
| `compaction_threshold` | fraction | the share of a window at which a session compacts (domain/session.md, section 8) |
| `parallel_tools` | count | the most adjacent reads of one completion that run at once, as one batch; the reads after them wait, in call order, until every call of that batch has answered, and then form the next batch. A session uses the smaller of it and `calls_per_response` (domain/session.md, section 5). It is from 1 to 8, the most calls a session runs at once |
| `connect`, `handshake` | duration, per endpoint | connecting, and TLS's handshake (section 7) |
| `connection_keep` | duration, per endpoint | how long an idle connection stays open in a live service; never how the process ends, since the pool closes at the answer (skein's `llm-connection.md`, section 7) |
| `tool_deadline` | duration | a tool call's longest deadline: the most a command's call may ask for, and the most any tool call's deadline may be, a host tool's included (domain/tools.md, section 5) |
| `shell_timeout` | duration | a command's deadline when its call asks for none |
| `job_poll` | duration | a `job` call's wait when it asks for none: the default of `wait_seconds` (domain/tools.md, section 5; added 2026-10-20) |
| `job_wait` | duration | the longest a `shell` call's yield or a `job` call's wait may be: the ceiling of `yield_seconds` and of `wait_seconds` (domain/tools.md, section 5; revised 2026-10-20, when it was also the default) |
| `check_timeout` | duration | the checks' deadline (domain/run.md, 8.1), cut at the run's end; a charter that allows a change keeps at least this much time in its reserve (domain/run.md, 9.1) |
| `group_stop` | duration | each of the three steps that stop a command's group (agent.md, section 2) |
| `close` | duration | io's close deadline (section 7) |
| `write_deadline` | duration | the most one write of the event stream may stay in flight before its destination is given up (events.md, section 5.2) |
| `wrap_up` | duration | the most time a sub-agent keeps for its final turn (domain/run.md, 5.3); a child's allowance is the smaller of this and a quarter of its effective time |
| `cancel_grace` | duration | the inline agent's grace for a cancel, before it drops a run (domain/host.md, 9.2) |
| `live_interval` | duration | the longest the agent holds live bytes before sending them as a piece (channel.md, section 7); the default is 100 ms |
| `exit_grace` | duration | a host's grace for a spawned agent to exit after its answer, above the agent's teardown bound (section 4) |
| `refresh_lead` | duration | how long before a sign-in record's access token expires the local host refreshes it: skein's `oauth.md` refresh lead, shared by every process on the record (`docs/design/shell.md`, 6.5 and 7.3). A story raises it so that its run refreshes at once (`docs/design/testing.md`, 2.3; added 2026-10-20) |

### 2.4 Fixed by the vocabulary

A few bounds are the vocabulary's own, sealed by its schemas and the same
in every deployment:

- **a delivery's diagnostic:** the last 512 bytes of the host's
  diagnostic output, with the count of bytes dropped before them
  (domain/run.md, 8.2);
- **a run's identity:** 16 opaque bytes (domain/run.md, 3.2;
  domain/host.md, section 2);
- **a credential:** a grant's value and what travels beside it, such as
  an account's id, within the bound the channel's schema seals for a
  grant (channel.md, section 6). Every source the local host lends from
  is read within it, and the request head's limit counts it at that
  bound (3.1);
- **an operator's line:** one line on standard error, a failure's
  detail clipped to its first 512 bytes at a character's boundary
  (agent.md, section 6);
- **a piece of live content:** at most 4 KiB of bytes, plus its fixed
  fields (a turn, a retry, a kind, an offset, and for an input the
  call's provider id and tool's name, each within the vocabulary's
  bound), the channel schema's (channel.md, section 4);
- **a model's effort:** a name of at most 32 bytes, the charter schema's
  (charter.md, section 2). The list of names a model accepts is its
  declaration (`docs/design/shell.md`, 6.1), never the vocabulary's;
- **an account's refusal:** the refused notice's code, text of at most
  128 bytes, skein's report bound, left empty past it, never clipped
  (skein's `llm.md`, 2.5 and 7); and its reason, text of at most 512
  bytes, the failure's detail cut at a character's boundary. Both are
  the channel schema's, and a failed run's refused account carries the
  code within the same bound (channel.md, section 4).

## 3. The derivation

Each layer derives its own `Limits` in its own crate, from the
declarations and from the limits of the layers it depends on, with
checked arithmetic: `None` on overflow, which refuses the configuration
naming the limit it was deriving. A derivation documents its factors
(a byte per token, an escape's expansion, a frame's overhead) where it
uses them. The service composes the chain once at startup and checks the
relationships between layers (section 4). Domains derive from what they
are given and never from a lower layer's types: the service passes each
the numbers it needs.

**How bounds compose.** A worst case is the most that can hold at once,
not the product of maxima that cannot. Every derivation follows these
rules:

1. **Joint bounds are charged at their shared total.** Items drawn from
   one shared quantity are charged at that quantity, plus a fixed cell
   per item, never as their count times the largest item. A completion's
   text, call arguments and copied identifiers all come out of one
   answer, `output` tokens at `TOKEN_BYTES` each. A batch's problems
   all come out of one contract's names.
2. **Escape factors apply only to encodings.** `ESCAPE` and a
   rendering's expansion apply to bytes on the wire or in rendered text.
   Decoded content held in memory is charged as decoded. The ×4
   rendering of invalid UTF-8 applies only to bytes that may be invalid.
3. **Counts come from in-memory items.** skein's containers allocate
   their capacity when made, so a count is memory. A count bounded by a
   byte budget is that budget over the smallest item as it is held in
   memory (its cell), never over the smallest item as encoded on the
   wire.
4. **One factor per direction.** Where a token count bounds bytes (a
   window's history at its threshold, an answer, a request), a token is
   `TOKEN_BYTES` bytes. Where a byte count bounds tokens (a fresh
   window, a room), a byte is one token. One comparison never mixes the
   two. `TOKEN_BYTES` is skein's decoded bytes per declared token;
   `smith-domain-session::ENCODED_BYTES` is the 5/4 allowance for escaping
   and framing in a batch's estimate of room below the line
   (domain/session.md, 8.1). The guard still measures the exact encoding.
5. **Queues are charged at what can be queued at once:** their capacity
   in cells, plus the payload all their producers can have queued at
   once (sessions times one record, say). Never their capacity times the
   largest payload.
6. **Frames follow this side's limits.** The channel's and io's limits
   are derived from the agent's own derived limits and from the
   direction a frame travels. A stream's output holds the largest frame
   this side writes. A schema's ceilings bound what may be declared,
   never what is reserved. The product's values set them: where a
   profile's value would pass a ceiling, the ceiling rises in place while
   formats are pre-release (README.md, section 8), as far as the codec's
   worst case at the ceilings allows (skein's `codec.md`, section 4).
   Many large items are bounded jointly, as one field of bytes at their
   total, as the start's transcript is (3.6), where a list's count times
   its largest item would pass that worst case.

Each derivation states which rule it follows where it combines bounds.
The service's tests hold, for the product's values, that every check
passes and that the complete worst case fits `memory`, recording the
number.

### 3.1 The LLM path: skein

`skein_llm::Limits::derive(&Declared)` (skein's `llm.md`, sections 4.1
and 4.2) runs once per endpoint. Its `Declared` takes the endpoint's
maxima, the largest `window`, `output` and `reasoning_item` of the models
the configuration declares for it, and the deployment's `tool_payload`,
`calls_per_response` and `conversations`. Its two factors, an escape's
expansion and the bytes a token may stand for, are skein's.

It gives every limit of the LLM path, per dialect: the request, the
answer, one call's input, a reasoning item, replay's metadata, a
completion's output items, the strings it keeps, the tokens it retains,
a call's receiving reservation, the bytes it skips, the tools and the
history items. Events are decoded as they stream and selectively
(skein's `json.md`), so what a provider echoes that smith never reads
costs a scan, not memory, and no limit is sized by it. Text smith sends
has no cap of skein's: smith bounds what it renders (3.4), and the
request bounds the whole.

skein's LLM connection component is given, from the same declarations
(skein's `llm-connection.md`, sections 3 and 7):

- **`calls`:** `conversations`, the calls it holds at once, running or
  waiting;
- **`connections`:** `conversations` across the pool, and as many per
  endpoint. skein checks connections ≥ calls when the component is made
  (skein's `llm.md`, section 4.3), so the service refuses to start
  otherwise;
- **`memory`:** `llm_pool`, the bytes calls may reserve at once;
- **`idle keep`:** each endpoint's `connection_keep`;
- **the request head:** measured from the endpoint's headers, the
  affinity's fixed rendering and a credential at its bound (2.4).

### 3.2 The session

`smith-domain-session` derives, from skein's limits and the
declarations:

- **a completion's blocks:** its calls, at most `calls_per_response`,
  and the few text, refusal and reasoning blocks a dialect adds;
- **one completion's output:** the charter's `output`, at most the
  model's;
- **a window's history:** the messages, blocks and bytes one window may
  hold, set so that the window's check is reached before any of them
  (domain/session.md, 8.1). Its bytes come from the window's side, by
  rule 4: the line's tokens at `TOKEN_BYTES` bytes each (3.9), plus what
  the window must receive before its next check, which is one
  completion's whole reserve, its answer included, since no output is
  reserved below the threshold, and one batch's results: the
  calls that run together, at most `parallel_tools` (domain/session.md,
  section 5), each at its render bound. A completion's later batches run
  within what is left (domain/session.md, 8.1), never in more history. So `window-first`
  holds by construction. Its messages and blocks are
  those bytes over a block's cell in memory, by rule 3, and they are
  also skein's declared history items (skein's `llm.md`, 4.2). A prompt
  the session sends is within skein's request, because the line bounds
  it in tokens and the request is derived from the same window. A
  request-size `Limit` cannot follow a passed admission: should one
  fire, a fault in this derivation, the session takes it as *context too
  long* (domain/session.md, section 4). With
  compaction, a session holds one window, so this is what a session
  holds, not what a run accumulates;
- **what a window holds at its opening, and what a compaction carries:**
  section 3.9;
- **the prompt's allowance:** what the protocol layer and the provider add
  to a prompt beyond the bytes the session counts, from the LLM
  component's framing, for the window's check and the budget's
  reservation (domain/run.md, section 9). The guard needs no share of it:
  it measures a request's new part with its framing, as encoded (3.9);
- **a completion's bytes,** by rules 1 and 2:
  - its answer once, for its text and its calls' encoded arguments;
  - once more for the decoded arguments and the identifiers copied from
    it, unescaped and at most the answer's bytes in all;
  - its reasoning at skein's share for it, output items × the reasoning
    bound, each item within that bound. A run of consecutive reasoning
    items is one block (skein's `llm.md`, section 2), so one block may
    hold the whole share;
  - a cell per block.

  Its reserve is the completion's bytes plus one answer's bytes for the
  details copied from it, and a slot per block, never twice the
  completion;
- **a decoded call's bytes:** `calls_per_response` decoded calls, each
  within one call's input and all within one answer's bytes (rule 1);
- **a session's jobs:** at most `jobs`, each with its number, the
  provider's id of the `shell` call that started it, its start's time,
  and whether its end has come; a cell each, since their kept output is
  the machine's (3.5). The session also keeps the highest number it has
  given, so that none is reused (domain/tools.md, section 5);
- **a batch's results:** each result within its render bound. Problems
  that name a contract's fields are charged at that contract's total
  (rule 1), rendered ×4 only where bytes may be invalid UTF-8 (rule 2);
- **a failure's detail,** the last failure's, kept for the operator: the
  provider's text within the largest failure detail skein's dialects
  keep (skein's `llm.md`, 4.2), or a limit's name and bound. The session
  keeps one, each later failure replacing it (domain/session.md, section
  4); the event stream carries it whole under content capture, and the
  operator's line clips it (2.4).

### 3.3 The transcript

`smith-transcript`'s schema seals ceilings; the agent's limits sit within
them (transcript.md, section 7):

- **a message's blocks:** from `calls_per_response`, the largest of:
  - a completion's blocks, skein's output items: twice
    `calls_per_response`, and two;
  - a message of results: `calls_per_response` results, then what is
    appended after them, one offer of messages (3.9) and the run's
    notes;
  - a window's opening: the task's blocks, its instruction and one
    offer, then the summary preamble's block, the summary's and one
    offer of held-back messages.

  So one declared number bounds both the completion and the message of
  its results, in the transcript (transcript.md, section 6);
- **a turn's bytes:** from a completion's answer, its calls' inputs, its
  results at their render bounds and its reasoning, one reasoning block
  holding at most the completion's share (3.2), which the schema's
  opaque block holds by rule 6;
- **a transcript's,** the resume limit: at least one window's turns, as
  a session holds them, and their bytes, since a transcript carries only
  the current window (transcript.md, section 7). Its turns are counted
  from the window's messages, each turn holding at least one, never from
  a turn budget: a window may outlast an activation.

### 3.4 Rendering

`smith-protocol-llm` derives one rendered result's bound (llm.md,
section 8):

- **each workspace tool's outcome at its caps,** rendered: its kept bytes,
  with every escape at its largest expansion, and what the rendering
  adds (line numbers, paths, the head and tail's separator, a clamp's
  note, a job's number and how to read it). A `running` answer and a
  `job` call's answer are outcomes like these: a job's kept output at
  `shell_head` and `shell_tail`, and its end or `running`;
- **a host tool's answer,** with the prefix the rendering adds; its text
  is at most `tool_payload`, like any piece the model reads at once;
- **a message,** with its label's prefix, at its bound (3.9);
- **a sub-agent's result:** its answer cap, the marker that cuts it, and
  the usage line;
- **the cut's marker,** which section 6's truncation adds.

The system text is not a result: the run writes it, and a window's room
bounds it (3.9).

### 3.5 The machine

`smith-protocol-machine` derives from the tools' limits, never beside
them (agent.md, section 2):

- **a command's captured output:** at least `shell_head` and
  `shell_tail`;
- **its processes:** at least the listings, searches, commands and
  checks the run's calls may have running at once, a batch of at most
  `parallel_tools` in each of `conversations`, and beside them `jobs`
  for each of `conversations`, each job with its process record;
- **a job's kept output:** `shell_head` + `shell_tail` bytes, the head
  and the tail of what it wrote since its last answer, and the count of
  bytes dropped between them, held from its `running` answer until an
  answer reports its end or its session closes;
- **a file's load and store:** at least `file_bytes`;
- **a scan and a search:** at least `list_entries`, `search_hits` and
  `search_bytes`;
- **paths:** at least the tools domain's path bound;
- **the environment:** the configured environment's encoding, exactly
  (agent.md, section 3).

### 3.6 The channel

`smith-protocol-channel` derives its half's limits, and with them its
terms (channel.md, sections 2 and 7):

- **a turn's frame:** at least the largest turn (3.3);
- **a start's frame:** the charter at its run bound, one window's
  transcript, the answers it lacks and the start's messages. The
  transcript is one piece of bytes, bounded at the resume limit's bytes
  with a length for each turn; the agent counts its turns against the
  resume limit's turns as it splits it (channel.md, section 3);
- **a message's frame:** a message at its bound (3.9);
- **its queue:** the window's turns, the calls in flight (at most
  `conversations`), the notices, the long operations, the answer and a
  reserve for facts and pieces, charged by rule 5 of section 3;
- **live content,** channel.md, section 7. Three numbers, all for the
  run's main conversation, the only one that gives any:
  - **the piece:** at most 4096 bytes (2.4), a frame of that and its
    fixed fields. A delta longer than it goes in several pieces;
  - **the interval:** `live_interval`, 100 ms (2.3). A piece goes when
    its bytes reach the bound or the interval ends since the oldest
    unsent byte, whichever comes first;
  - **the held bytes:** four pieces, 16 KiB, by rule 5: what can wait
    at once for a channel with no room is the retry's shared text and call
    bytes. Each piece contains one stream only. Past it the newest bytes are dropped
    and counted (channel.md, section 7). A piece is cut from the held
    bytes, so nothing else in this path is charged. The queue's reserve
    for pieces is one piece's frame, since the agent sends one piece at
    a time. The service's sum adds the held bytes once (3.8, section 5).
    Identities are bounded at 16,385: one per retained byte plus the open
    call, each with at most 256 id bytes and 64 tool bytes. Their map
    cells, payload and the held bytes' stream/offset cursors are counted
    by `live::worst_case`; the host keeps one bounded input identity;
- **by direction,** by rule 6. What the agent writes is bounded by its
  own derived limits. What it reads is bounded by what it admits: a
  start within the charter's run bound, a message within its bound. The
  schema's ceilings only bound what may be declared.

The host's terms are checked against these at the opening, not at
startup: a host that takes less finds out before a run starts.

### 3.7 The event sink

The largest record follows from the capture policy (events.md, section
5): under `everything`, a response's prompt delta is at most one turn's
growth, its results at their render bounds. The sink's output cap holds
that record and the room it reserves for authoritative records.

### 3.8 The service

`smith-agent-service`, and `smith-local-service` for a run in one
process, compose the chain, run the startup checks, and sum the worst
cases (section 5). Each holds its profiles as step code (README.md,
section 5).

**Admission by a watermark** (skein's programming-model.md, 7.1). The queues between the service's own children (the
domain's events and requests, the LLM's, the machine's, IO's and the
file driver's) are uncapped: no child reserves room in another, and
startup no longer asks `queue` to hold any child's largest output. Two
kinds of queue keep their caps, as edges: the three the host's channel
fills and drains, and the kernel's submissions and completions, which
the shell fills from outside the service. New work, the host's `Start`
and each `Message`, waits in the channel's queue while the service's
backlog, the entries in its uncapped queues, is at `queue`; everything
that advances admitted work is taken at any backlog, still waiting on
real credit (a destination's, the channel's to the host, the file
table, the kernel's queue). `queue` also bounds each drain's work per
iteration. One order the room checks used to impose is stated instead:
a domain entrance (a resume, an event, a timer) waits until the
previous entrance's requests have been dispatched, so its facts never
overtake the turn they follow; and a domain entrance waits until the
previous one's facts have reached the event sinks (`has_facts`). The
domain's facts queue is uncapped, so that wait is the facts' only bound,
and the domain events it holds meanwhile fill the backlog. The children
below (the domain, the inline agent, the host domain) reserve no room
for their own children; each exposes `backlog()` for the root above it,
and only the service has a watermark.

In the local service, a person's line or slash command is its new work:
it waits at the terminal's head while the backlog is at the watermark,
`service.watermark` (by default `queue`); the answers to admitted work
never wait. A queue the shell reaches only through a service method is
internal and uncapped; its edges are the queues it hands out (`&mut
Queue`) and the terminal's input. The backlog counts the queues between
the service's children, and what a child holds through its `backlog()`
(the local domain's counts its agent's, and drains its spawned agent's
host facts, which nothing local reads); the inline effects expose
theirs, and the credential owner keeps its queues. Two orders are stated:
a local entrance waits for the previous entrance's requests (an
interrupt or a drop passes it), and a store's actions go out before the
next file terminal.

### 3.9 A window's room

A window is counted in tokens and most of what fills it in bytes. A
token is at least one byte, so a byte count bounds a token count: these
bounds count each byte as one token, and so does the guard, which
measures a request as encoded. The session's check estimates instead:
it counts the bytes appended since its last prompt at that window's
measured ratio (domain/session.md, 8.1). Where the material, a byte a
token, would not fit, the derivation bounds what a window holds, never
the window, which is the model's.

For each model the configuration declares:

- **The room** is its `window` at `compaction_threshold`, less two
  `output`s: one for the summary that opens a new window, one for the
  output of that window's first completion, so that the check holds it
  below the threshold (domain/session.md, 8.1). The summary counts as
  the tokens its compaction reported, or `output` when none was, never
  as its bytes.
- **The line** is its `window` less one `output`, the summary
  instruction and a cut result's marker for each of `calls_per_response`
  calls. No ordinary prompt is sent past it: the guard measures what a
  request adds since its window's last prompt as encoded, its framing
  included, a byte a token: the request's encoded body less that of the
  same request cut where the new part starts. It refuses, unsent, a
  request whose last prompt and that measure pass the line
  (domain/session.md, 8.1; llm.md, section 6). The compaction after a
  prompt within the line, carrying that prompt's answer of at most
  `output`, fits the window.
  - **The material above the line,** the summary instruction's bytes
    plus `calls_per_response` cut markers' bytes, a byte a token, is one
    derived limit of the run's, `run.compaction_material`. The run gives
    each conversation its line when it opens it: the charter's effective
    `window` for its model, less its `output` and that material. So a
    reader of the event stream finds each conversation's line from its
    model's `window_tokens` and `output_tokens` in `conversation.opened`
    and the material among the effective limits (section 9; events.md,
    3.4 and 3.10).
- **The spare** is what lies between the threshold and the line: the
  window above `compaction_threshold` less one `output`, the summary
  instruction and the markers, what `compaction-headroom` leaves (section
  4). It is the room the session's estimate may miss by before the guard
  refuses a request. A charter's effective `window` and `output` give the
  line and the spare its session uses.
- **smith's own part** is taken first: the run's sections at their
  largest, smith's tools' schemas, the opening's and the summary's
  instructions, the summary preamble, and the prompt's allowance (3.2).
  smith's code fixes them, and the service measures them at startup. Only what is the same
  whatever the charter says is smith's own. A number a section shows,
  such as a budget's, counts at its largest.
- **The rest is split in halves,** a factor of the derivation's own:
  - **the host's part:** every byte that is there because of what the
    charter chose, and its directories' guides. That includes:
    - the charter's instructions, the task role among them when the
      shell gives it (`docs/design/shell.md`, 6.6), its brief, and its
      host tools' descriptions and schemas;
    - what smith renders from the charter's choices, in the system text
      and in the schemas it generates for `finish` and `deliver`: the
      contract's forms, fields, rules, verdict labels and items; the
      workspace's directory names, conflicts and convention paths;
      everything smith adds around each of them.

    It is measured as rendered, at the entrance, never at the
    vocabulary's maxima. A start whose charter alone passes it is
    refused at the entrance.
  - **The guides share what the charter leaves.** Each of the start's
    directories keeps its guide whole up to its share: the host's part
    less the charter as rendered, split evenly among the start's
    directories, and never more than `guide`. Past its share a guide is
    cut as past `guide` (domain/run.md, 3.3). So a start with one
    directory keeps the guide the product sizes, and a start with
    several fits whatever their guides hold: more directories cut their
    guides sooner, never refuse the start. The count is the start's
    directories, never the run's `directories` limit, which bounds only
    how many a start may name.
  - **How the run counts what the protocol renders.** The system text
    is the run's own to write, so the run counts it directly. The
    generated schemas are `smith-protocol-llm`'s to render, and the run
    cannot call the renderer without a cycle, so the run counts them
    from plain numbers in its limits:
    - **what the numbers are:** the schemas' fixed bytes, each kind of
      element's bytes besides its own text, and each byte's escaped
      length;
    - **where they come from:** the service measures them at startup
      with the protocol's renderer, as it measures smith's own part;
    - **every entrance uses them.** The root's, the inline agent's and
      the spawned agent's entrances all run the run's admission, so
      each is guarded, and none needs a measurement from the protocol
      at the entrance;
    - **a property keeps them exact:** the count equals the rendered
      bytes for drawn contracts. A sub-agent's brief takes the charter's place, and a
    `sub_agent` call whose brief passes it is answered as too large
    (llm.md, section 4);
  - **the window's messages:** two offers (below). The task is the
    opening's instruction and one offer, and a new window carries the
    task, the summary preamble, counted in smith's own part, the summary
    and at most one offer of held-back messages.
- **One offer** is the most the run gives the model at once (domain/run.md,
  sections 3.4 and 6): at most `inbox` messages, and at most half the
  window's messages in rendered bytes. What an offer leaves stays
  queued, in order, for the next one.
- **A message's bound** is at most one offer's bytes, and at most
  `tool_payload`. So every message a run admits fits an offer and can be
  read, and a new window always has room for one.

The derivation takes the smallest room among the declared models, so one
set of bounds holds whichever model a charter names. A charter may lower
a model's `window` or `output`, but not so far that its room falls below
the one the limits were derived for (charter.md, section 6).

A window closes by compaction: its history, the results owed to it, and
the summary instruction, then a summary of up to `output`, the
compaction's output, beside its prompt (2.1; domain/session.md, 8.2).
The guard keeps each prompt sent within the line, and its output is at
most `output`, so the history a compaction carries leaves above it at
least the summary instruction and a marker for each owed result. **The results owed to a
compaction** are rendered within what the window leaves after the
history before them and the summary instruction; each is cut with its
marker where they would pass it (llm.md, section 5). History before
them is unchanged, so the compaction still reads the cached prefix, but
for what its tool choice costs on the Anthropic dialect (llm.md, 2.2).

## 4. Checked at startup

Each relationship has a name. A configuration that breaks one is
refused before the loop starts, and the refusal names the relationship,
the limits on each side and their values (agent.md, section 6;
`smith check` in `docs/design/shell.md`).

skein checks its own as it derives and composes, each refused by its
name (skein's `llm.md`, section 4.3): one call's input within the
answer; the output items and strings its rules give; one largest call's
request and receiving reservation within `llm_pool`; at least as many
connections as `conversations`; each demand within its cap; and
Anthropic's output within what its route accepts. smith checks the rest:

| Name | Relationship |
|---|---|
| `derived` | every derived limit fits its type; the first that overflows is named |
| `fresh-window` | for each declared model, a new window at its largest (smith's own part, the host's part, the task as its instruction and one offer, a summary of `output` tokens, and one offer of held-back messages) and one `output` fit within the window at the compaction threshold, counting a byte as a token (3.9). The parts are derived so that this holds whenever smith's own part fits the room, for every shipped preset among them (`docs/design/shell.md`, 6.1); the check refuses a model whose room smith's own part fills |
| `host-part-holds-the-role` | for each declared model, the host's part (3.9) holds the task role as written, its bytes as the shell's code holds them, not its 4 KiB cap (`docs/design/shell.md`, 6.6), beside one guide at `guide`, the shell's own start's one directory, so that the product's own default never cuts its guide or makes a start too large for the entrance; a start with more directories shares the rest (3.9) |
| `compaction-headroom` | for each declared model, the window above its threshold holds one `output` (that of the completion whose results a compaction carries), the summary instruction and a cut result's marker for each of `calls_per_response` results: the line is at or above the threshold, and the compaction the check or the guard calls for always fits, its results cut to what is left; what it leaves is the spare (3.9; domain/session.md, 8.1) |
| `output-in-window` | each model's `output` is less than its `window` |
| `window-first` | a window's history reaches the compaction threshold before its message, block or byte bound: the line at `TOKEN_BYTES` bytes a token, plus one completion's whole reserve and one batch's results, within the history's bytes (rule 4 of section 3). It holds by construction for 3.2's derivation, and guards it |
| `calls-in-transcript` | a message's blocks, derived from `calls_per_response`, are at least a completion's blocks and within the transcript schema's ceiling |
| `resume-holds-window` | the resume limit holds the most a session holds in one window |
| `turn-in-transcript` | the largest turn fits the transcript schema's ceilings |
| `turn-in-channel` | the largest turn, start and message fit the channel's frame ceilings |
| `render-in-result` | every tool's outcome at its caps, a host's answer, a message and a sub-agent's result render uncut within one rendered result's bound: the cut is a safeguard, never what a cap relies on (llm.md, section 5). A bound held below its derived worst case by any other limit breaks it |
| `read-in-payload` | `read_window` is at most `tool_payload` |
| `payload-in-file` | `tool_payload` is at most `file_bytes` |
| `machine-holds-tools` | the machine's output, file, scan, search and path limits hold the tools', and its processes hold the listings, searches, commands and checks that run at once, and `jobs` for each of `conversations` beside them (3.5) |
| `environment` | the configured environment's names are valid and distinct |
| `request-head` | a request's head holds a credential at its bound (2.4), the affinity's headers and the endpoint's configured headers, none of which is a reserved name |
| `ceilings-in-vocabulary` | the policy ceilings fit the charter's and the channel's schemas |
| `deadlines` | every deadline is positive, `shell_timeout` ≤ `tool_deadline`, so no command's deadline, asked for or not, passes what the session allows one tool call, and `job_poll` ≤ `job_wait` ≤ `tool_deadline`, so no yield or wait does either, the default included |
| `memory` | the sum of every worst case, with the LLM pool at its size, fits `memory` |
| `cancel-over-close` | `cancel_grace` is at least twice io's `close`: a cancelled call settles within one `close` against a peer that keeps its connection open (skein's `shell.md`, section 13), so a grace no longer than `close` drops a run whose cancel would have settled; the second `close` is the margin |

Two more hold across processes, and the side that knows both checks
them:

| Name | Who checks | Relationship |
|---|---|---|
| `grace-over-teardown` | a host that writes its agent's configuration | `exit_grace` exceeds the agent's teardown bound (section 7), with a margin, as skein's `shell.md`, section 13 has a supervisor derive its grace (agent.md, section 6; domain/host.md, section 4) |
| `watchdog-over-silence` | the host kit | its no-progress deadline exceeds the longest deadline the run may be silent for between reports: io's short operations and its own steps (domain/host.md, section 4) |

What a run asks for is checked at its entrance, not at startup
(domain/run.md, section 4): a model the configuration does not declare;
each model entry's `window` and `output` against the model's
declaration, its room against the one the limits were derived for, and
its window above the threshold against one `output` and
`run.compaction_material`, so that its spare is not negative (3.9),
refused as a window or an output above its declaration is (charter.md,
section 6); the charter's text against the host's part (its directories' guides
take their shares of what it leaves, 3.9); the budget against the ceilings; and the
reserve against the budget.

## 5. Memory

- **A real target.** `memory` is what the process may use. The product's
  presets keep it within the real memory a person's machine can spare
  (`docs/design/shell.md`). The shell refuses to start when the worst
  case exceeds it (programming-model.md, section 6.3). Within what a
  machine can spare, the cap is as large as the work needs (below).
- **A cap, not an allocation.** What the process allocates follows what
  it holds, not the worst case:
  - bytes are allocated when they arrive, at their final length, and
    freed with their owner when its use ends: a compacted window's
    history once the new window opens, a job's kept output once an
    answer has carried it, a closed conversation's history and calls;
  - capacity reserved up front, slabs' and queues' (programming-model.md,
    section 6.1), is the exception: sized by the limits, counted in the
    worst case, held for the process's life. A queue whose cap holds more
    than 1 MiB grows to it instead, priced at its growth peak, and the
    service's uncapped queues (3.8) are priced at `queue` entries each,
    an estimate (skein's lib.md 5).
- **Room is made by raising the cap.** A change that grows the worst
  case names, in its commit, each part that grew, by how many bytes, and
  what the worst case leaves under `memory`. One whose worst case passes
  `memory` raises the profile's `memory` to hold it; one whose worst
  case still fits leaves `memory` as it is, with no margin of its own
  kept above the worst case. A change never shaves another part's
  accounting to fit. The startup check of the worst case stays.
- **Use is measured, beside the cap.** The worlds' counting allocator
  holds the live heap within the worst case at every iteration, and
  shows it returning when a use ends (section 11). The benchmarks read
  smith's own peak resident memory beside its time and cost
  (benchmarks.md, 16.1).
- **Admission to the LLM pool.** Every LLM call in flight draws on
  `llm_pool`, which skein's connection component keeps (skein's
  `llm-connection.md`, section 7). At admission a call reserves its
  request, measured exactly before it is encoded, and its receiving
  reservation, which skein derives (skein's `llm.md`, section 4.2); it
  releases both at its terminal. A call that does not fit what is left
  waits, in order of arrival, within its deadline (section 6).
- **So the LLM path's worst case** is the pool's size, plus each
  connection's own state (its socket's buffers, TLS) and the component's
  tables. It no longer grows with the request's bound times the number of
  connections.
- **The rest is counted at its derived limits:** each conversation's
  window of history; the channel's queue and frames; the machine's
  captures, with `jobs` for each of `conversations`, each at its kept
  output and its process record (3.5); each write in flight's file and
  its result, `file_bytes` each; io's buffers; the event sink's output
  cap. Each layer's `worst_case` adds what its containers report
  (programming-model.md, section 6.3).
- **Waiting is the backpressure.** A full pool slows the run's calls; it
  never refuses one that fits the pool alone (programming-model.md,
  section 7).
- **One process counts each part once.** The product in one process
  (`docs/design/shell.md`, section 9) has one loop, one ring and no
  channel. Its worst case is the sum of what it owns, each part once:
  - the local host's domain with the inline agent, which holds the run's
    domains;
  - the effects the run uses (the LLM component, the machine, the files,
    the event sink), built from their own parts, never as a whole agent
    service built and then stripped, so nothing is allocated or priced
    for a domain or a channel they do not hold;
  - one io, for the local host and the effects together;
  - the chat store, the terminal and delivery.

  No agent process and no host channel are priced. Construction holds no
  more than the worst case: what is never kept is never built.

## 6. One call at a limit

| What | Outcome |
|---|---|
| a tool call whose arguments pass one call's input | the call arrives as an oversize block with its byte count; the completion completes; the model is told the size, the bound, and how to make it fit (llm.md, section 4) |
| a rendered result past its bound | cut at a character's boundary with an explicit marker saying how many bytes were left out; the request goes (llm.md, section 5) |
| a call that finds the pool's connections or memory at capacity | it waits, in order of arrival, bounded by `conversations`, within its deadline (llm.md, section 6) |
| a reasoning item past `reasoning_item` | a typed limit failure, unless the model's declaration drops it (2.1) |
| a tool call cut off by the output limit | marked by skein; the session keeps a note and an error result in its place (llm.md, section 4) |
| history that reaches the window's threshold | a compaction, not a failure (domain/session.md, section 8) |
| results owed to a compaction past what its headroom leaves | cut with their markers in the compaction's request only (3.9) |
| a completion's later batch past what is left of the history | each call runs with its share of what is left as its cap, its result saying what it left out; the next completion is the compaction (domain/session.md, 8.1) |
| a completion's batch past what is left below the line | each call runs with its share of what is left as its cap, its result saying what it left out; the next completion is sent when the guard's exact measure fits (domain/session.md, 8.1) |
| messages queued past one offer | they wait, in order, for the next offer (3.9) |
| a provider's "context too long" | one compaction; a second in the same window fails the session |
| a completion past `calls_per_response` | a typed limit failure of the completion; the tools' descriptions state the bound |
| a `shell` call with `yield_seconds` while its session holds `jobs` jobs | answered as an invalid call that says at most `jobs` may run and how to stop one; the command is not run (domain/tools.md, section 5) |
| a yield or a wait asked past `job_wait` | clamped to it, and the outcome says so |
| what a job writes between two answers, past its kept output | its head and tail kept, and the bytes between them counted and said (3.5) |
| a message, a host's answer, a charter or a start past its bound | refused at its entrance, naming the bound (domain/run.md, sections 4 and 6) |

## 7. Deadlines

| Deadline | Runs | Its value | On expiry |
|---|---|---|---|
| connect | from the connection's start to its socket connected | the endpoint's | transient |
| handshake | from connected to TLS ready | the endpoint's | transient |
| head | from the request sent to the response's first byte | the model's `head` | transient |
| idle | between two events of the response | the model's `idle` | transient |
| whole | the call | the session's remaining time | not retried: the session's time is up |

- **Progress, not duration.** A response that keeps streaming is never
  cut by a fixed bound. There is no ceiling per attempt.
- **`TimedOut` carries its phase:** `Connect`, `Handshake`, `Head`,
  `Idle` or `Whole` (skein's `llm-connection.md`). The session retries by
  phase (domain/session.md, section 4).
- **A call waiting for the pool** is within its whole bound; past it, it
  fails unsent.
- **Tools and checks** have the domain's deadlines: a host tool's from
  the charter, at most `tool_deadline`; a command's from its call, or
  `shell_timeout`, clamped to `tool_deadline`; the checks' is
  `check_timeout`, cut at the run's end (domain/tools.md, section 5).
- **A job keeps its command's deadline,** counted from the command's
  start, whatever its yield. A `shell` call's yield and a `job` call's
  wait bound how long that call waits before it answers, never how long
  the job runs, and are at most `job_wait`.
- **A cancel of jobs settles within the group stop.** A session's close,
  like whatever takes the lease (domain/run.md, sections 5.3 and 8.1),
  stops the groups of the jobs it cancels together, each in the three
  steps of agent.md, section 2, so it waits at most three `group_stop`s
  for them, however many there are.
- **The end of the process** is bounded only by io's close deadlines
  (skein's `shell.md`, section 13). After its answer the agent closes its
  components together, its LLM connections, its machine's roots and its
  channel, and then its event stream, after `session.ended` (agent.md,
  section 6): two `close` deadlines in turn, the stream's
  `write_deadline` for its last records, and the moment its cancels take
  to settle, which io's close deadlines already bound and which adds
  nothing to them. That is its **teardown bound**, from which a host
  derives `exit_grace` (section 4).
- **Jobs add nothing to the teardown bound.** A session's close cancels
  its running jobs and waits for them to settle (domain/session.md,
  section 5), and the run answers
  only once its sessions have closed and what was in flight has settled
  (domain/run.md, section 4), so the machine holds a running job at its
  close only after a run dropped without an answer. Its close stops each
  such job with io's close of the job's child, the group stop's third
  step, within one `close` (agent.md, section 2).

## 8. A limit that fires

- **Typed.** A limit that fires is `Limit { which, bound }`: which limit,
  and its value. The LLM path's kinds are skein's (skein's `llm.md`);
  smith's are its own layers' (a session's history, a decoded call, a
  message). An HTTP failure carries its status.
- **Carried whole.** The session keeps it in its failure, the run in its
  answer (domain/run.md, section 10), the event stream in
  `response.completed` and `run.completed` (events.md, section 3), and
  the operator sees it in words: on standard error (agent.md, section
  6), and in the local host's account of the failure (hosts.md, section
  5.1).
- **Refusals name what they compared.** A startup refusal names the
  relationship (section 4). An entrance refusal names the bound and the
  value asked.

## 9. Effective limits

The declared values, the policy values and every derived limit, each
under its name in its unit, are the **effective limits**. The declared
and policy values are named as section 2 names them; a derived limit is
named by its layer and its field in that layer's `Limits`, such as
`llm.request` or `session.history_bytes`. A value per endpoint or per
model adds them by their configured names, never by a number:
`llm.request[endpoint=codex]`, `declared.head[endpoint=codex,model=gpt-6-luna]`,
so a reader joins them to the names in `conversation.opened`. Sizes are
in bytes or tokens as their names say; durations in whole milliseconds. `smith check` prints them (`docs/design/shell.md`, 4.4), and the
event stream's header carries them (events.md, section 3.2). Their names
and units are part of the event vocabulary, so a change to them is a new
version of it (events.md, section 6).

## 10. What the domain is owed, and what skein owes

- **The domains are owed** their limits as numbers, derived before the
  loop, with every relationship between layers already checked, so no
  domain meets another layer's limit in the middle of a run.
- **skein owes:**
  - `Limits::derive` for the LLM path, with its documented factors and
    its own relationships checked (skein's `llm.md`, section 4);
  - selective, streamed decoding of provider events (skein's `json.md`
    and `http.md`);
  - an oversized call as a block of its own, typed `Limit` kinds and
    HTTP statuses, and a reasoning bound apart from replay's metadata
    (skein's `llm.md`);
  - waiting for a connection or for room in the memory pool, the expired
    phase in `TimedOut`, and a pool sized from the owner's concurrency
    (skein's `llm-connection.md`, sections 4.1, 6 and 7);
  - each container's `worst_case` (programming-model.md, section 6.3).

## 11. The world

- **Derivation properties,** in each deriving crate's step tests: for
  declared values drawn at random within their types, derivation either
  refuses, naming one relationship, or gives limits for which every
  relationship of section 4 holds. Nothing panics or wraps.
- **Each relationship's refusal:** one configuration per row of section
  4 that breaks it, refused with its name.
- **Every profile:** each shipped profile, with every preset model
  declared, derives, passes every check, `fresh-window` and
  `compaction-headroom` included, and its worst case fits its `memory`.
- **A window's room,** in the session's world with small windows: a
  window just opened at its largest, its first completion made; more
  messages queued than one offer holds, the rest offered at the next
  yield; a compaction whose owed results pass its headroom, cut and
  fitting; a charter too large for the host's part, refused.
- **No pinned values.** Tests assert properties of the derivation, never
  that a limit equals a number. Worlds declare tiny values
  (testing-strategy.md, section 3), so every limit is reached in a world.
- **The LLM path,** in the protocol world `tests/protocol-llm` (llm.md,
  section 10): calls waiting at a full pool, then served; an oversized
  call beside an ordinary one; a reasoning item at its bound and one past
  it; a response streaming past any fixed bound; a stall timed out
  `Idle` and retried; the session's time ending once, without a retry.
- **Rendering:** a result past its bound cut with its marker; random
  outcomes at random caps rendered within the bound.
- **Live content:** a stream longer than the held bytes against a
  channel with no room, dropped past the bound and counted; pieces never
  larger than the piece bound; a piece sent at the interval's end with
  fewer bytes than the bound (channel.md, section 10).
- **Memory:** skein's counting allocator against the formula, with the
  LLM pool full and every conversation open, each holding `jobs` jobs at
  their kept output's ceiling, in the agent's simulated world (agent.md,
  section 8).
- **Memory returns** (section 5), with the same allocator, in the
  agent's simulated world:
  - **a compaction:** the live heap falls, once the new window opens, by
    at least the payload bytes of the compacted history's blocks less
    those of the new window's opening, cells left out, since a window's
    message list keeps its capacity;
  - **repeated uses:** across a story that compacts, reads jobs to
    their end, and opens and closes conversations, several times each,
    the live heap at the same point of each repetition is no larger than
    at the first.
- **Jobs and batches,** in the session's world with small values: a
  `shell` call with a yield refused while `jobs` jobs are held; a yield and a
  wait clamped to `job_wait`; a wait asked for none, `job_poll`; more
  adjacent reads than `parallel_tools`,
  the rest run as the next batch once the first has answered.

## 12. From temper

- **Kept:** limits per layer, refusal at the entrance, and the worst case
  checked at startup (programming-model.md, sections 6 and 7).
- **New:** declared quantities and their derivation, relationships
  checked by name, the LLM pool, and limits as per-call outcomes.

## 13. Open questions

- **Capacity reserved up front:** how much of smith's measured peak is
  slabs' and queues' capacity, allocated at startup at the limits'
  bounds (programming-model.md, section 6.1). Whether that rule should
  give way to capacity allocated as it fills is skein's question, which
  jig and temper share; smith measures first.
- **Calls past `calls_per_response`:** whether skein marks the calls
  beyond the bound as calls of their own, answered as not run, so the
  completion completes, as an oversized call's does.
- **A ceiling per attempt:** whether a model's declaration may add one,
  for a provider that streams without end.
- **A host answer's own bound:** whether host tools' answers get a
  quantity of their own rather than `tool_payload`. Messages now take
  theirs from one offer (3.9).
- **A window's halves:** the room left after smith's own part goes half
  to the host's text and half to messages. Proposed as a first split;
  the history probe measures whether a host's text or a person's
  messages want more of it; what it gives the presets is
  `docs/design/shell.md`'s, 6.1.
- **The rest of the codecs' ceilings:** whether the channel's and the
  transcript's decoding also draws on a shared pool, as LLM calls do, or
  its worst case at its derived limits stays small enough.
