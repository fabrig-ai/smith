//! Named startup comparisons use independent declared values and exact refusals.

mod role;

use alloc::boxed::Box;
use skein_lib::Duration;

use super::{
    Check, Configuration, Endpoint, Layer, LimitName, Model, OversizedReasoning, Per, Refusal, Value, standard,
    validate, words,
};

pub(super) fn profile() -> super::Profile {
    let mut profile = standard();
    profile.declared.memory = u64::MAX;
    profile.declared.tool_payload = 256;
    profile.declared.read_window = 256;
    profile.declared.calls_per_response = 2;
    profile
}

pub(super) fn declarations() -> Configuration {
    Configuration {
        role_bound: None,
        environment_bytes: 0,
        endpoints: Box::new([Endpoint {
            number: 7,
            provider: skein_llm::Provider::OpenAiCodex,
            option_bytes: 0,
            connect: Duration::from_secs(2),
            handshake: Duration::from_secs(3),
            models: Box::new([Model {
                efforts: Box::new([]),
                name: Box::from(b"one".as_slice()),
                window: 65_536,
                output: 512,
                reasoning_item: 2048,
                oversized_reasoning: OversizedReasoning::Fail,
                head: Duration::from_secs(5),
                idle: Duration::from_secs(11),
            }]),
        }]),
    }
}

#[test]
fn a_turn_holds_a_reasoning_block_at_the_completions_share() {
    use smith_transcript::{Opaque, OpaqueParts};
    let selected = standard();
    let mut configuration = declarations();
    configuration.endpoints[0].models[0].window = 1_048_576;
    configuration.endpoints[0].models[0].output = 32_768;
    configuration.endpoints[0].models[0].reasoning_item = 65_536;
    let derived = super::derive(&selected, &configuration).expect("standard profile");
    let client = derived.llm.adapter.client;
    let transcript = derived.channel.transcript;
    let share = client.output_items.checked_mul(client.reasoning).expect("completion share");
    assert_eq!(share, 2_228_224);
    let envelope = share + skein_llm::REPLAY_HEADER_BYTES;
    assert_eq!(transcript.opaque_bytes, envelope);
    let bytes = skein_lib::bytes::zeroed(usize::try_from(envelope).expect("envelope fits"));
    let block = Opaque::new(&transcript, OpaqueParts { dialect: 1, bytes })
        .expect("one block holds consecutive reasoning items");
    assert_eq!(block.bytes().len(), usize::try_from(envelope).expect("envelope fits"));
    let mut smaller = transcript;
    smaller.opaque_bytes -= 1;
    let problem = Opaque::new(&smaller, block.into_parts()).expect_err("one byte beyond the bound");
    assert_eq!(problem.path, smith_transcript::v6::Path::OpaqueBytes);
}

#[test]
fn a_small_window_lowers_the_guide_bound_and_admits_the_run() {
    use skein_lib::Token;
    use smith_domain::run;
    let mut selected = standard();
    selected.declared.calls_per_response = 1;
    selected.declared.tool_payload = 8192;
    selected.declared.read_window = 8192;
    let mut configuration = declarations();
    configuration.endpoints[0].models[0].output = 8192;
    let limits = super::derive(&selected, &configuration).expect("small-window profile").domain.run;
    assert!(u64::from(limits.guide_bytes) <= limits.host_part);
    assert!(limits.guide_bytes < selected.declared.guide);
    let settings =
        smith_agent_world::Settings { job: smith_agent_world::Job::Reporting, ..smith_agent_world::Settings::calm(0) };
    let mut charter = smith_agent_world::scripted_charter(&settings);
    charter.llm.output = 8192;
    charter.models = Box::default();
    let workspace = run::Workspace {
        directories: Box::new([run::Directory {
            name: b"work".as_slice().into(),
            host: b"/workspace".as_slice().into(),
            root: Token::new(1),
            writable: true,
            git: false,
            conflicts: Box::default(),
        }]),
    };
    let mut out = start_with_workspace(charter, workspace, &limits);
    let Some(run::Request::Admitted { .. }) = out.pop() else { panic!("charter admitted") };
    let Some(run::Request::Read { max, .. }) = out.pop() else { panic!("guide read") };
    assert!(max <= limits.guide_bytes);
}

fn start_with_workspace(
    charter: smith_domain::run::Charter,
    workspace: smith_domain::run::Workspace,
    limits: &smith_domain::run::Limits,
) -> skein_lib::Queue<smith_domain::run::Request> {
    use skein_lib::{Env, Queue, ReplyTo, Time, Token, Wall};
    use smith_domain::run;
    let mut domain = run::Domain::new(limits, 0);
    let mut out = Queue::with_capacity(run::MAX_OUT);
    run::step(
        &mut domain,
        &Env { now: Time::ZERO, wall: Wall::EPOCH, limits: *limits },
        run::Event::Start {
            messages: Box::default(),
            reply_to: ReplyTo::new(Token::new(1)),
            host_run: Token::new(1),
            identity: None,
            activation: 1,
            window: run::Window { turns: u32::MAX, bytes: u64::MAX, largest_turn: 1 },
            charter,
            workspace: Some(workspace),
            transcript: None,
            resumed: false,
        },
        &mut out,
    );
    out
}

#[test]
fn the_standard_refresh_lead_is_sixty_seconds() {
    assert_eq!(standard().policy.refresh_lead, Duration::from_secs(60));
}

#[test]
fn a_zero_refresh_lead_names_its_policy_key() {
    let mut selected = profile();
    selected.policy.refresh_lead = Duration::ZERO;
    let refusal = super::derive(&selected, &declarations()).expect_err("zero refresh lead");
    assert_eq!(words(&refusal).as_ref(), b"deadlines: policy.refresh_lead=0 ns; policy.deadline_minimum=0 ns");
}

/// A signed-in plan's access token is a JWT of some kilobytes: each grant value the standard
/// profile's channel admits, its LLM component holds.
#[test]
fn the_standard_component_holds_each_grant_value_its_channel_admits() {
    let limits = super::derive(&profile(), &declarations()).expect("standard profile");
    assert!(limits.llm.grant_value_bytes >= limits.channel.bodies.credential_value_bytes);
}

#[test]
fn the_standard_exit_grace_exceeds_its_teardown_bound() {
    let selected = profile();
    let mut limits = super::derive(&selected, &declarations()).expect("standard profile");
    limits.events = Some(
        crate::EventLimits::derive(&limits, crate::sinks::Capture::None, 1, 0, &[], selected.policy.write_deadline)
            .expect("standard event stream"),
    );
    assert_eq!(selected.policy.exit_grace, Duration::from_secs(45));
    assert_eq!(super::teardown_bound(&limits), Some(Duration::from_secs(40)));
    assert!(selected.policy.exit_grace > super::teardown_bound(&limits).expect("bounded teardown"));
}

#[test]
fn an_exit_grace_at_the_teardown_bound_names_the_broken_relationship() {
    let mut selected = profile();
    selected.policy.exit_grace = Duration::from_secs(40);
    let refusal = super::derive(&selected, &declarations()).expect_err("no exit margin");
    assert_eq!(refusal.check(), Some(Check::GraceOverTeardown));
    assert_eq!(
        words(&refusal).as_ref(),
        b"grace-over-teardown: policy.exit_grace=40000000000 ns; service.teardown=40000000000 ns",
    );
}

#[test]
fn file_bounds_follow_the_tools_admitted_piece_and_listing() {
    let mut selected = profile();
    let mut configuration = declarations();
    configuration.endpoints[0].models[0].output = 4096;
    for (payload, entries) in [(256, 3), (1024, 17), (4096, 100)] {
        selected.declared.tool_payload = payload;
        selected.declared.read_window = payload / 2;
        selected.declared.list_entries = entries;
        let limits = super::derive(&selected, &configuration).expect("derived file bounds");
        assert_eq!(limits.file_read, payload);
        assert_eq!(limits.file_entries, entries);
        assert!(limits.file_slots >= limits.machine.roots + limits.machine.operations);
        assert_eq!(limits.file_timeout, limits.domain.session.tools.file_timeout);
    }
}

#[test]
fn io_slots_hold_connections_the_channel_files_and_process_pipes() {
    let mut selected = profile();
    for conversations in [1, 2, 6, 17] {
        selected.declared.conversations = conversations;
        let limits = super::derive(&selected, &declarations()).expect("derived io bounds");
        let required = conversations + 3 + limits.file_slots + 3 * limits.machine.processes;
        assert!(limits.io.sockets >= required);
        assert_eq!(limits.llm.connection.io, limits.io);
    }
}

#[test]
fn io_slot_overflow_names_its_derived_limit() {
    let selected = profile();
    let limits = super::derive(&selected, &declarations()).expect("derived io bounds");
    assert_eq!(
        super::io_limits(128, &selected.policy, u32::MAX, &limits.machine, limits.file_slots),
        Err(Refusal::Derived { limit: LimitName::deployment(Layer::Io, "sockets") }),
    );
}

#[test]
#[expect(
    clippy::disallowed_methods,
    reason = "UTF-8 fixture slices feed an independent test encoder, programming-model.md section 10.2"
)]
fn a_payload_sized_write_fits_the_stated_whole_call_input() {
    let mut selected = profile();
    selected.declared.tool_payload = 40_960;
    selected.declared.read_window = 32_768;
    let mut configuration = declarations();
    configuration.endpoints[0].models[0].output = 32_000;
    configuration.endpoints[0].models[0].window = 272_000;
    let limits = super::derive(&selected, &configuration).expect("product call limits");
    let stated = limits.llm.adapter.stated;
    let mut path_bytes = [1_u8; 256];
    path_bytes[127] = b'/';
    let path = core::str::from_utf8(&path_bytes).expect("admitted UTF-8 path");
    assert_eq!(path.len(), 256, "the derived path ceiling");
    for byte in [b'a', b'\n', 0] {
        let mut content = skein_lib::bytes::zeroed(40_960);
        content.fill(byte);
        let content = core::str::from_utf8(&content).expect("admitted UTF-8 content");
        let input =
            serde_json::to_vec(&serde_json::json!({"path": path, "content": content})).expect("valid write JSON");
        assert!(
            input.len() <= usize::try_from(stated.call_input_bytes).expect("input fits usize"),
            "a payload-sized write needs {} encoded bytes, but the stated whole-call bound is {}",
            input.len(),
            stated.call_input_bytes
        );
    }
}

#[test]
fn write_envelopes_derive_checked_room_for_different_piece_sizes() {
    let mut selected = profile();
    let mut configuration = declarations();
    configuration.endpoints[0].models[0].output = 32_000;
    let framing = u32::try_from(br#"{"path":"","content":""}"#.len()).unwrap();
    for payload in [1, 2, 17, 256, 4096, 40_960] {
        selected.declared.tool_payload = payload;
        selected.declared.read_window = payload;
        let tools = super::derive_tools(&selected).expect("derived tools path");
        let client = super::endpoint_client(&selected, &configuration.endpoints[0], 2).expect("call envelope");
        let encoded = (payload + tools.path_bytes) * skein_llm::ESCAPE + framing;
        assert!(client.input >= encoded, "the complete Write envelope fits");
        assert!(client.input - encoded < skein_llm::ESCAPE, "rounding adds less than one escape");
    }
    selected.declared.tool_payload = u32::MAX;
    let refusal = super::endpoint_client(&selected, &configuration.endpoints[0], 2).expect_err("input overflow");
    assert_eq!(refusal, Refusal::Derived { limit: LimitName::deployment(Layer::Llm, "input") });
}

#[test]
fn declared_whole_file_read_reaches_the_kernel_open() {
    use skein_io::{file, file_layer, kernel};
    use skein_lib::{Queue, Time, Token};

    for whole_file in [1_048_576, 65_537, 65_536, 256] {
        let mut profile = profile();
        profile.declared.file_bytes = whole_file;
        let limits = super::derive(&profile, &declarations()).expect("valid whole-file declaration");
        let mut files = file_layer::FileIo::with_whole_limit(
            limits.file_slots,
            limits.file_read,
            limits.file_entries,
            limits.file_bytes,
            limits.file_timeout,
        );
        let descriptor = kernel::Fd::new(7);
        let root = files.adopt_root(descriptor).expect("root slot");
        let owner = Token::new(11);
        let mut events = Queue::with_capacity(4);
        let mut submissions = Queue::with_capacity(4);
        file_layer::down(
            &mut files,
            Time::ZERO,
            file::Request::Load {
                owner,
                root,
                path: Box::from(b"README.md".as_slice()),
                max: limits.domain.session.tools.file_bytes,
                no_follow: false,
            },
            &mut events,
            &mut submissions,
        );
        assert!(events.pop().is_none(), "whole-file cap {whole_file} refused before open");
        let open = submissions.pop().expect("admitted load opens its file");
        assert_eq!(
            open.kind,
            kernel::Op::Open { root: descriptor, path: Box::from(b"README.md".as_slice()), how: kernel::OpenHow::Read }
        );
        assert!(submissions.pop().is_none());
    }
}

#[test]
fn read_past_payload_retains_both_names_values_and_units() {
    let mut profile = profile();
    profile.declared.read_window = 1025;
    profile.declared.tool_payload = 1024;
    let refusal = super::derive(&profile, &declarations()).expect_err("read exceeds payload");
    assert_eq!(refusal.check(), Some(Check::ReadInPayload));
    assert_eq!(
        words(&refusal).as_ref(),
        b"read-in-payload: declared.read_window=1025 bytes; declared.tool_payload=1024 bytes"
    );
}

#[test]
fn output_at_the_window_names_the_actual_endpoint_and_model() {
    let mut configuration = declarations();
    let model = configuration.endpoints.get_mut(0).expect("one endpoint").models.get_mut(0).expect("one model");
    model.window = 8192;
    model.output = 8192;
    let refusal = super::derive(&profile(), &configuration).expect_err("output must be below window");
    let per = Per::Model { endpoint: 7, model: 0 };
    assert_eq!(
        refusal,
        Refusal::Broken {
            check: Check::OutputInWindow,
            left: LimitName { layer: Layer::Declared, field: "output", per }.value(Value::Tokens(8192)),
            right: LimitName { layer: Layer::Declared, field: "window", per }.value(Value::Tokens(8192)),
        }
    );
    assert_eq!(
        words(&refusal).as_ref(),
        b"output-in-window: declared.output[endpoint=7,model=0]=8192 tokens; declared.window[endpoint=7,model=0]=8192 tokens"
    );
}

#[test]
fn zero_model_idle_is_a_named_deadline_refusal() {
    let mut configuration = declarations();
    configuration.endpoints.get_mut(0).expect("one endpoint").models.get_mut(0).expect("one model").idle =
        Duration::ZERO;
    let refusal = super::derive(&profile(), &configuration).expect_err("zero idle");
    assert_eq!(refusal.check(), Some(Check::Deadlines));
    assert_eq!(
        words(&refusal).as_ref(),
        b"deadlines: declared.idle[endpoint=7,model=0]=0 ns; policy.deadline_minimum=0 ns"
    );
}

#[test]
fn shell_default_cannot_pass_its_tool_deadline() {
    let mut profile = profile();
    profile.policy.shell_timeout = Duration::from_secs(11);
    profile.policy.job_poll = Duration::from_secs(10);
    profile.policy.job_wait = Duration::from_secs(10);
    profile.policy.tool_deadline = Duration::from_secs(10);
    let refusal = super::derive(&profile, &declarations()).expect_err("shell exceeds tool deadline");
    assert_eq!(
        words(&refusal).as_ref(),
        b"deadlines: policy.shell_timeout=11000000000 ns; policy.tool_deadline=10000000000 ns"
    );
}

#[test]
fn policy_fields_past_the_vocabulary_are_refused_before_deriving() {
    let mut profile = profile();
    profile.policy.fields = smith_charter::CEILINGS.run_result_fields.checked_add(1).expect("small ceiling");
    let refusal = super::derive(&profile, &declarations()).expect_err("field ceiling");
    assert_eq!(refusal.check(), Some(Check::CeilingsInVocabulary));
    assert_eq!(
        words(&refusal).as_ref(),
        b"ceilings-in-vocabulary: policy.fields=33 items; channel.text_rule_fields=32 items"
    );
}

#[test]
fn composed_machine_refuses_a_file_cap_below_the_tools() {
    let mut profile = profile();
    profile.declared.file_bytes = 512;
    let limits = super::derive(&profile, &declarations()).expect("valid declarations");
    let mut machine = limits.machine;
    machine.file_bytes = limits.domain.session.tools.file_bytes.checked_sub(1).expect("positive payload");
    let refusal = validate::machine(&machine, &limits.domain).expect_err("file cap");
    assert_eq!(refusal.check(), Some(Check::MachineHoldsTools));
    assert_eq!(
        words(&refusal).as_ref(),
        b"machine-holds-tools: tools.file_bytes=512 bytes; machine.file_bytes=511 bytes"
    );
}

#[test]
fn a_larger_inbox_does_not_raise_the_rooms_offer_bound() {
    let mut profile = profile();
    let original = super::derive(&profile, &declarations()).expect("bounded room").domain.run;
    profile.policy.inbox = u32::MAX;
    let larger = super::derive(&profile, &declarations()).expect("bounded offer").domain.run;
    assert_eq!(larger.messages, u32::MAX);
    assert_eq!(larger.offer_messages, u32::MAX);
    assert_eq!(larger.offer_bytes, original.offer_bytes);
    assert_eq!(larger.message_bytes, original.message_bytes);
}

#[test]
fn native_connection_relationship_keeps_its_actual_values() {
    let refusal = Refusal::Connection(skein_llm_connection::EndpointError::HttpReadTlsRead { demand: 31, cap: 30 });
    assert_eq!(refusal.check(), None);
    assert_eq!(words(&refusal).as_ref(), b"http-read-tls-read: demand=31 bytes; cap=30 bytes");
}

#[test]
fn random_declarations_refuse_by_name_or_hold_the_checked_relationships() {
    let mut rng = skein_lib::Rng::new(19405);
    for _ in 0_u32..256 {
        let mut profile = profile();
        let mut configuration = declarations();
        profile.declared.tool_payload = u32::try_from(rng.next_u64() % 4096 + 1).expect("payload");
        profile.declared.read_window = u32::try_from(rng.next_u64() % 8192 + 1).expect("read");
        profile.declared.file_bytes = u32::try_from(rng.next_u64() % 8192 + 1).expect("whole file");
        let model = configuration.endpoints.get_mut(0).expect("one endpoint").models.get_mut(0).expect("one model");
        model.window = u32::try_from(rng.next_u64() % 8192 + 1).expect("window");
        model.output = u32::try_from(rng.next_u64() % 8192 + 1).expect("output");
        let output = model.output;
        let window = model.window;
        match super::derive(&profile, &configuration) {
            Ok(limits) => {
                assert!(profile.declared.read_window <= profile.declared.tool_payload, "default read fits payload");
                assert!(profile.declared.tool_payload <= profile.declared.file_bytes, "payload fits file");
                assert!(output < window, "output fits window");
                validate::resume_holds_window(
                    u64::from(limits.domain.session.history_messages),
                    u64::from(limits.channel.resume_turns),
                )
                .expect("window turns fit resume");
                validate::turn_in_channel(
                    u64::from(limits.channel.bodies.turn_body),
                    u64::from(smith_channel::CEILINGS.turn_body),
                )
                .expect("turn fits channel");
                assert!(limits.channel.transcript.listed_entries >= limits.domain.session.tools.list_entries);
                validate::machine(&limits.machine, &limits.domain).expect("machine holds tools");
            }
            Err(Refusal::Llm { endpoint, violation: _ }) => assert_eq!(endpoint, 7, "native refusal retains its scope"),
            Err(refusal) => assert!(refusal.check().is_some(), "smith refusals retain their relationship"),
        }
    }
}

#[test]
fn a_tool_piece_must_fit_the_separately_declared_whole_file() {
    let mut profile = profile();
    profile.declared.read_window = 128;
    profile.declared.tool_payload = 256;
    profile.declared.file_bytes = 255;
    let refusal = super::derive(&profile, &declarations()).expect_err("payload past file");
    assert_eq!(refusal.check(), Some(Check::PayloadInFile));
    assert_eq!(
        words(&refusal).as_ref(),
        b"payload-in-file: declared.tool_payload=256 bytes; declared.file_bytes=255 bytes"
    );
    profile.declared.file_bytes = 256;
    validate::declarations(&profile, &declarations()).expect("payload at file ceiling");
    profile.declared.file_bytes = 1_u32 << 20_u32;
    let limits = super::derive(&profile, &declarations()).expect("independent file declaration");
    assert_eq!(limits.domain.session.tools.file_bytes, 1_u32 << 20_u32);
    assert_eq!(limits.domain.session.tools.tool_payload, 256);
    assert_eq!(limits.machine.file_bytes, 1_u32 << 20_u32);
}

#[test]
fn zero_child_wrap_up_is_a_named_deadline_refusal() {
    let mut selected = profile();
    selected.policy.wrap_up = Duration::ZERO;
    let refusal = super::derive(&selected, &declarations()).expect_err("zero child wrap-up");
    assert_eq!(refusal.check(), Some(Check::Deadlines));
    assert_eq!(words(&refusal).as_ref(), b"deadlines: policy.wrap_up=0 ns; policy.deadline_minimum=0 ns");
}

#[test]
fn window_first_refuses_each_history_dimension_by_its_name() {
    let profile = profile();
    let configuration = declarations();
    let original = super::derive(&profile, &configuration).expect("coherent tiny declarations").domain.session;
    for field in ["history_bytes", "history_messages", "history_blocks"] {
        let mut limits = original;
        match field {
            "history_bytes" => limits.history_bytes = 1,
            "history_messages" => limits.history_messages = 1,
            "history_blocks" => limits.history_blocks = 1,
            _ => unreachable!(),
        }
        let refusal = validate::window_first(&profile, &configuration, &limits).expect_err("too small before window");
        match refusal {
            Refusal::Broken { check: Check::WindowFirst, right, .. } => {
                assert_eq!(right.name.field, field);
                assert_eq!(right.name.per, Per::Model { endpoint: 7, model: 0 });
            }
            refusal @ (Refusal::Field { .. }
            | Refusal::Broken { .. }
            | Refusal::Derived { .. }
            | Refusal::Environment { .. }
            | Refusal::Connection(_)
            | Refusal::Llm { .. }) => panic!("unexpected refusal: {refusal:?}"),
        }
    }
}

#[test]
fn request_head_and_uncut_rendering_keep_the_actual_byte_sides() {
    validate::request_head(7, 100, 100).expect("head exactly fits");
    let refusal = validate::request_head(7, 101, 100).expect_err("head exceeds transport piece");
    assert_eq!(refusal.check(), Some(Check::RequestHead));
    assert!(words(&refusal).ends_with(b"llm.request_piece[endpoint=7]=100 bytes"));
    validate::render_in_result(100, 100).expect("whole result fits");
    let refusal = validate::render_in_result(101, 100).expect_err("cut cannot satisfy startup");
    assert_eq!(words(&refusal).as_ref(), b"render-in-result: render.uncut_result=101 bytes; render.result=100 bytes");
}

#[test]
fn window_first_holds_owned_and_delegated_batches_at_the_exact_byte_edge() {
    const COMPLETION_BYTES: u64 = 4096;
    const RESULT_BYTES: u64 = 100_000;
    // (65536 window - 512 output - (415 summary + 2 * 37 markers)) * 8.
    const LINE_BYTES: u64 = 516_280;
    let profile = profile();
    let configuration = declarations();
    let path = super::path::path(&profile, &configuration).expect("native declarations");
    let mut limits = super::derive_domain(&profile, &configuration, &path, COMPLETION_BYTES, COMPLETION_BYTES)
        .expect("numeric domain limits")
        .session;
    limits.tools.list_bytes = RESULT_BYTES;
    limits.delegated_result_bytes = 1;
    let reserve = smith_domain::session::completion_reserve(&limits).expect("completion reserve");
    let batch =
        smith_domain::tools::batch_result_worst_case(&limits.tools, limits.parallel_tools).expect("owned batch");
    assert_eq!(batch, 200_000, "two largest read results coexist");
    limits.history_bytes = LINE_BYTES + reserve + batch;
    validate::window_first(&profile, &configuration, &limits).expect("exact receiving edge");
    limits.history_bytes -= 1;
    let refusal = validate::window_first(&profile, &configuration, &limits).expect_err("one byte beyond history");
    assert_eq!(refusal.check(), Some(Check::WindowFirst));
    limits.history_bytes += 1;
    limits.delegated_result_bytes = RESULT_BYTES + 1;
    let refusal = validate::window_first(&profile, &configuration, &limits).expect_err("delegated batch is larger");
    assert_eq!(refusal.check(), Some(Check::WindowFirst));
    limits.delegated_result_bytes = u64::MAX;
    assert_eq!(
        validate::window_first(&profile, &configuration, &limits),
        Err(Refusal::Derived { limit: LimitName::deployment(Layer::Session, "batch_results") })
    );
    limits.delegated_result_bytes = 1;
    limits.tools.list_bytes = u64::MAX;
    assert_eq!(
        validate::window_first(&profile, &configuration, &limits),
        Err(Refusal::Derived { limit: LimitName::deployment(Layer::Session, "batch_results") })
    );
}

#[test]
fn window_first_refuses_a_history_one_byte_short_of_the_line() {
    const COMPLETION_BYTES: u64 = 4096;
    let profile = profile();
    let configuration = declarations();
    let path = super::path::path(&profile, &configuration).unwrap();
    let mut limits =
        super::derive_domain(&profile, &configuration, &path, COMPLETION_BYTES, COMPLETION_BYTES).unwrap().session;
    limits.history_bytes -= 1;
    let refusal = validate::window_first(&profile, &configuration, &limits).unwrap_err();
    assert_eq!(refusal.check(), Some(Check::WindowFirst));
    assert!(words(&refusal).starts_with(b"window-first: session.line[endpoint=7,model=0]="));
}

#[test]
fn transcript_blocks_hold_the_largest_completion_or_name_the_ceiling() {
    let profile = profile();
    let limits = super::derive(&profile, &declarations()).expect("coherent declarations");
    assert!(limits.channel.transcript.message_blocks >= limits.domain.session.completion_blocks);
    let refusal = validate::calls_in_transcript(65, 64).expect_err("one beyond the vocabulary");
    assert_eq!(refusal.check(), Some(Check::CallsInTranscript));
    assert_eq!(
        words(&refusal).as_ref(),
        b"calls-in-transcript: session.message_blocks=65 items; transcript.message_blocks=64 items"
    );
}

#[test]
fn the_worst_case_does_not_grow_with_connections_times_the_request() {
    let mut selected = profile();
    selected.declared.memory = 2_147_483_648;
    let mut configuration = declarations();
    for conversations in [1, 2, 4, 6] {
        selected.declared.conversations = conversations;
        let small = super::derive(&selected, &configuration).expect("small window");
        configuration.endpoints[0].models[0].window *= 2;
        let large = super::derive(&selected, &configuration).expect("large window");
        assert_eq!(small.llm.connection.memory, large.llm.connection.memory);
        assert_eq!(large.llm.connection.memory, selected.declared.llm_pool);
        let small_client = small.llm.adapter.client;
        let large_client = large.llm.adapter.client;
        assert!(large_client.request > small_client.request);
        assert_eq!(skein_llm::client::worst_case(&small_client), skein_llm::client::worst_case(&large_client));
        configuration.endpoints[0].models[0].window /= 2;
    }
}

#[test]
fn a_turn_past_the_transcript_ceiling_is_refused_by_name() {
    let mut selected = profile();
    selected.declared.list_entries = 513;
    let refusal = super::derive(&selected, &declarations()).expect_err("listing exceeds transcript");
    assert_eq!(refusal.check(), Some(Check::TurnInTranscript));
    assert_eq!(
        words(&refusal).as_ref(),
        b"turn-in-transcript: session.listed_entries=513 items; transcript.listed_entries=512 items"
    );
}

#[test]
fn a_resume_smaller_than_the_window_is_refused_by_name() {
    let refusal = validate::resume_holds_window(65, 64).expect_err("one turn short");
    assert_eq!(refusal.check(), Some(Check::ResumeHoldsWindow));
    assert_eq!(
        words(&refusal).as_ref(),
        b"resume-holds-window: session.history_messages=65 items; channel.resume_turns=64 items"
    );
}

#[test]
fn a_turn_larger_than_the_channel_frame_is_refused_by_name() {
    let refusal = validate::turn_in_channel(101, 100).expect_err("one byte short");
    assert_eq!(refusal.check(), Some(Check::TurnInChannel));
    assert_eq!(
        words(&refusal).as_ref(),
        b"turn-in-channel: session.frame_bytes=101 bytes; channel.frame_bytes=100 bytes"
    );
}

#[test]
fn cancel_grace_under_twice_close_is_refused_as_cancel_over_close() {
    let mut profile = profile();
    profile.policy.close = Duration::from_secs(5);
    profile.policy.cancel_grace = Duration::from_millis(9_999);
    let refusal = super::derive(&profile, &declarations()).expect_err("cancel grace lacks its close margin");
    assert_eq!(refusal.check(), Some(Check::CancelOverClose));
    assert_eq!(
        refusal,
        Refusal::Broken {
            check: Check::CancelOverClose,
            left: LimitName::deployment(Layer::Io, "twice_close").value(Value::Duration(Duration::from_secs(10))),
            right: LimitName::deployment(Layer::Policy, "cancel_grace")
                .value(Value::Duration(Duration::from_millis(9_999))),
        }
    );
}

#[test]
fn parallel_tools_outside_one_to_eight_is_refused() {
    for parallel_tools in [0, 9, u32::MAX] {
        let mut selected = profile();
        selected.policy.parallel_tools = parallel_tools;
        assert_eq!(
            super::derive(&selected, &declarations()).expect_err("parallel tools outside range"),
            Refusal::Field { limit: LimitName::deployment(Layer::Policy, "parallel_tools") },
            "parallel_tools={parallel_tools}"
        );
    }
}

#[test]
fn tool_batches_use_the_smaller_of_policy_and_response_calls() {
    for (parallel_tools, calls_per_response, width) in [(1, 16, 1), (4, 16, 4), (8, 16, 8), (8, 2, 2)] {
        let mut selected = profile();
        selected.policy.parallel_tools = parallel_tools;
        selected.declared.calls_per_response = calls_per_response;
        let limits = super::derive(&selected, &declarations()).expect("bounded parallel tools");
        assert_eq!(limits.domain.session.parallel_tools, width);
        assert_eq!(limits.domain.session.tools.calls, width);
    }
}

fn job_profile(parallel_tools: u32, calls_per_response: u32, jobs: u32) -> crate::Limits {
    let mut selected = profile();
    selected.declared.conversations = 3;
    selected.declared.calls_per_response = calls_per_response;
    selected.declared.jobs = jobs;
    selected.policy.parallel_tools = parallel_tools;
    super::derive(&selected, &declarations()).expect("batches, checks and jobs derive")
}

#[test]
fn the_machines_processes_hold_every_batch_check_and_job() {
    // Three conversations hold four calls and four jobs each, plus four directory checks.
    assert_eq!(job_profile(4, 16, 4).machine.processes, 28);
}

#[test]
fn without_jobs_the_processes_hold_single_calls_and_checks() {
    // Three conversations hold one call each, plus four directory checks.
    assert_eq!(job_profile(1, 16, 0).machine.processes, 7);
}

#[test]
fn response_calls_reduce_the_process_capacity_beside_held_jobs() {
    // Three conversations hold two calls and two jobs each, plus four directory checks.
    assert_eq!(job_profile(8, 2, 2).machine.processes, 16);
}

fn smaller_process_refusal(parallel_tools: u32, calls_per_response: u32, jobs: u32) -> Box<[u8]> {
    let limits = job_profile(parallel_tools, calls_per_response, jobs);
    let mut machine = limits.machine;
    machine.processes = machine.processes.checked_sub(1).expect("positive process count");
    words(&validate::machine(&machine, &limits.domain).expect_err("processes below required count"))
}

#[test]
fn a_machine_missing_one_batch_check_or_job_refuses() {
    assert_eq!(
        smaller_process_refusal(4, 16, 4).as_ref(),
        b"machine-holds-tools: machine.processes_required=28 items; machine.processes=27 items"
    );
}

#[test]
fn a_machine_without_jobs_still_holds_calls_and_checks() {
    assert_eq!(
        smaller_process_refusal(1, 16, 0).as_ref(),
        b"machine-holds-tools: machine.processes_required=7 items; machine.processes=6 items"
    );
}

#[test]
fn a_response_limited_batch_still_requires_every_job_slot() {
    assert_eq!(
        smaller_process_refusal(8, 2, 2).as_ref(),
        b"machine-holds-tools: machine.processes_required=16 items; machine.processes=15 items"
    );
}

#[test]
fn a_job_wait_above_the_tool_deadline_is_refused() {
    let mut selected = profile();
    selected.policy.tool_deadline = Duration::from_secs(10);
    selected.policy.shell_timeout = Duration::from_secs(10);
    selected.policy.job_poll = Duration::from_secs(10);
    selected.policy.job_wait = Duration::from_secs(11);
    assert_eq!(
        super::derive(&selected, &declarations()).expect_err("job wait exceeds tool deadline"),
        Refusal::Broken {
            check: Check::Deadlines,
            left: LimitName::deployment(Layer::Policy, "job_wait").value(Value::Duration(Duration::from_secs(11))),
            right: LimitName::deployment(Layer::Policy, "tool_deadline")
                .value(Value::Duration(Duration::from_secs(10))),
        }
    );
}

#[test]
fn a_zero_job_wait_is_refused_before_deriving_limits() {
    let mut selected = profile();
    selected.policy.job_wait = Duration::ZERO;
    assert_eq!(
        super::derive(&selected, &declarations()).expect_err("zero job wait"),
        Refusal::Broken {
            check: Check::Deadlines,
            left: LimitName::deployment(Layer::Policy, "job_wait").value(Value::Duration(Duration::ZERO)),
            right: LimitName::deployment(Layer::Policy, "deadline_minimum").value(Value::Duration(Duration::ZERO)),
        }
    );
}

#[test]
fn the_worst_case_counts_each_sessions_jobs_at_their_kept_output() {
    let mut selected = profile();
    selected.declared.conversations = 3;
    selected.declared.jobs = 0;
    let without = super::derive(&selected, &declarations()).expect("no jobs");
    selected.declared.jobs = 4;
    let with = super::derive(&selected, &declarations()).expect("four jobs per session");
    assert_eq!(without.machine.processes, 10);
    assert_eq!(with.machine.processes, 22);
    assert_eq!(with.domain.session.tools.jobs, 4);
    assert_eq!(with.domain.session.tools.job_poll, Duration::from_secs(30));
    assert_eq!(with.domain.session.tools.job_wait, Duration::from_secs(300));
    let mut without_machine = without.machine;
    let mut with_machine = with.machine;
    without_machine.search_bytes = 0;
    with_machine.search_bytes = 0;
    let before = smith_protocol_machine::worst_case(&without_machine).expect("bounded machine");
    let after = smith_protocol_machine::worst_case(&with_machine).expect("bounded machine");
    without_machine.output_bytes = 0;
    with_machine.output_bytes = 0;
    let before_records = smith_protocol_machine::worst_case(&without_machine).expect("bounded records");
    let after_records = smith_protocol_machine::worst_case(&with_machine).expect("bounded records");
    assert!(after_records > before_records, "each job also owns its process and pipe bookkeeping");
    // Twelve jobs keep two 48 KiB windows, above their 16 + 48 KiB kept output.
    assert_eq!((after - before) - (after_records - before_records), 1_179_648);
}

#[test]
fn listing_filter_translation_is_counted_once_per_step() {
    let mut selected = profile();
    selected.declared.conversations = 3;
    let mut machine = super::derive(&selected, &declarations()).expect("job profile").machine;
    assert_eq!((machine.operations, machine.processes), (2, 22));
    machine.file_bytes = 0;
    machine.tool_payload = 0;
    let smaller = smith_protocol_machine::worst_case(&machine).expect("bounded machine");
    machine.file_bytes = 1024;
    machine.tool_payload = 1024;
    let larger = smith_protocol_machine::worst_case(&machine).expect("bounded machine");
    // Two guide buffers, twenty-two retained filters and four translation buffers.
    assert_eq!(larger - smaller, 28_672);
}

#[test]
fn composed_machine_refuses_an_argument_cap_below_the_tools() {
    let limits = super::derive(&profile(), &declarations()).expect("valid declarations");
    let mut machine = limits.machine;
    machine.tool_payload = 255;
    let refusal = validate::machine(&machine, &limits.domain).expect_err("argument cap");
    assert_eq!(refusal.check(), Some(Check::MachineHoldsTools));
    assert_eq!(
        words(&refusal).as_ref(),
        b"machine-holds-tools: tools.tool_payload=256 bytes; machine.tool_payload=255 bytes"
    );
}

fn pathname_growth(processes: u32) -> u64 {
    let mut machine = super::derive(&profile(), &declarations()).expect("job profile").machine;
    machine.processes = processes;
    machine.search_line_bytes = 0;
    let smaller = smith_protocol_machine::worst_case(&machine).expect("bounded machine");
    machine.search_line_bytes = 1024;
    smith_protocol_machine::worst_case(&machine)
        .expect("bounded machine")
        .checked_sub(smaller)
        .expect("a larger line grows the bound")
}

#[test]
fn pathname_translation_copies_are_counted_only_for_one_step() {
    // Seven more processes retain three listing paths and one search line,
    // while the two pathname translation copies belong to one step.
    assert_eq!(pathname_growth(8) - pathname_growth(1), 28_672);
}

#[test]
fn a_zero_live_interval_is_refused() {
    let mut selected = profile();
    selected.policy.live_interval = Duration::ZERO;
    let refusal = super::derive(&selected, &declarations()).expect_err("zero live interval");
    assert_eq!(refusal.check(), Some(Check::Deadlines));
    assert_eq!(words(&refusal).as_ref(), b"deadlines: policy.live_interval=0 ns; policy.deadline_minimum=0 ns");
}

#[test]
fn a_credential_value_cap_is_worded_credential_value() {
    let refusal = Refusal::Connection(skein_llm_connection::EndpointError::Client(skein_llm::Error::Limit {
        which: skein_llm::Cap::Value,
        bound: 7,
    }));
    assert!(words(&refusal).ends_with(b"request-head: credential-value bound=7"));
}

#[test]
fn a_zero_job_poll_is_refused_before_deriving_limits() {
    let mut selected = profile();
    selected.policy.job_poll = Duration::ZERO;
    assert_eq!(
        super::derive(&selected, &declarations()).expect_err("zero job poll"),
        Refusal::Broken {
            check: Check::Deadlines,
            left: LimitName::deployment(Layer::Policy, "job_poll").value(Value::Duration(Duration::ZERO)),
            right: LimitName::deployment(Layer::Policy, "deadline_minimum").value(Value::Duration(Duration::ZERO)),
        }
    );
}

#[test]
fn a_job_poll_above_the_job_wait_is_refused() {
    let mut selected = profile();
    selected.policy.job_poll = Duration::from_secs(301);
    assert_eq!(
        super::derive(&selected, &declarations()).expect_err("job poll exceeds job wait"),
        Refusal::Broken {
            check: Check::Deadlines,
            left: LimitName::deployment(Layer::Policy, "job_poll").value(Value::Duration(Duration::from_secs(301))),
            right: LimitName::deployment(Layer::Policy, "job_wait").value(Value::Duration(Duration::from_secs(300))),
        }
    );
}

#[test]
fn a_job_poll_at_the_job_wait_is_accepted() {
    let mut selected = profile();
    selected.policy.job_poll = Duration::from_secs(300);
    let limits = super::derive(&selected, &declarations()).expect("default reaches its ceiling");
    assert_eq!(limits.domain.session.tools.job_poll, Duration::from_secs(300));
    assert_eq!(limits.domain.session.tools.job_wait, Duration::from_secs(300));
}

#[test]
fn standard_job_poll_and_job_wait_are_thirty_seconds_and_five_minutes() {
    let selected = standard();
    assert_eq!(selected.policy.job_poll, Duration::from_secs(30));
    assert_eq!(selected.policy.job_wait, Duration::from_secs(300));
}
