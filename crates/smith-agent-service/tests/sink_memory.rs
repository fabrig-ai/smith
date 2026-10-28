//! Two slow event destinations retain maximal escaped call identities while
//! every owning step is checked by Skein's allocator (events.md, section 5.3).
#[cfg(test)]
mod tests {
    #![allow(
        clippy::disallowed_macros,
        clippy::disallowed_methods,
        clippy::wildcard_enum_match_arm,
        reason = "ordinary Rust test fixtures and boundary assertions follow programming-model.md section 10.2"
    )]
    use skein_heap::{Counting, Meter};
    use skein_io as io;
    use skein_lib::{Queue, Time, Token, stream};
    use smith_agent_service::sinks::{Capture, Delivery, Destination, SinkLimits, Sinks};
    use smith_events::{Effect, Event, Record, Source, ToolStarted};

    #[global_allocator]
    static HEAP: Counting = Counting;

    fn product_limits() -> SinkLimits {
        use smith_agent_service::profile::{self, Configuration, Endpoint, Model, OversizedReasoning};
        let mut profile = profile::standard();
        profile.declared.tool_payload = 40960;
        profile.declared.file_bytes = 1_048_576;
        profile.declared.calls_per_response = 16;
        profile.declared.read_window = 32768;
        profile.declared.conversations = 6;
        profile.declared.guide = 16384;
        profile.declared.shell_output = 16384;
        profile.declared.shell_head = 16384;
        profile.declared.shell_tail = 49152;
        profile.declared.search_hits = 100;
        profile.declared.search_bytes = 16384;
        profile.declared.list_entries = 512;
        profile.declared.llm_pool = 134_217_728;
        profile.declared.memory = 2_281_701_376;
        let declarations = Configuration {
            role_bound: None,
            environment_bytes: 835,
            endpoints: Box::new([Endpoint {
                number: 1,
                provider: skein_llm::Provider::OpenAiCodex,
                option_bytes: 3,
                connect: skein_lib::Duration::from_secs(10),
                handshake: skein_lib::Duration::from_secs(10),
                models: Box::new([Model {
                    efforts: Box::new([]),
                    name: b"gpt-6-luna".as_slice().into(),
                    window: 272_000,
                    output: 32000,
                    reasoning_item: 65536,
                    oversized_reasoning: OversizedReasoning::Fail,
                    head: skein_lib::Duration::from_secs(60),
                    idle: skein_lib::Duration::from_secs(300),
                }]),
            }]),
        };
        let limits = profile::derive(&profile, &declarations).expect("source-admitted product fixture");
        smith_agent_service::EventLimits::derive(&limits, Capture::Everything, 1, 3, &[], profile.policy.write_deadline)
            .expect("actual joint product bounds")
            .sinks
    }

    #[test]
    #[expect(clippy::print_stdout, reason = "retain independent actual allocation and output-bound figures")]
    fn an_actual_full_escaped_prompt_fits_sink_output_and_allocation_at_every_chunk() {
        // Peak 24_577_858 B on 2026-10-28; a quarter of headroom, rounded up.
        const PROMPT_SINK_CEILING: u64 = 31_000_000;

        use skein_lib::List;
        let limits = product_limits();
        assert_eq!(
            limits.line_ceiling(Capture::Everything),
            Some(19_855_104),
            "V6 writer ceiling with the 415-byte summary instruction",
        );
        let mut messages = List::with_capacity(8);
        for _ in 0_u32..8 {
            let mut blocks = List::with_capacity(1);
            blocks
                .push(smith_events::Block::Text { text: vec![0; 256_000].into_boxed_slice() })
                .expect("actual blocks");
            messages.push(smith_events::Message { role: smith_events::Role::User, blocks }).expect("actual message");
        }
        let record = Record {
            t_ms: 1,
            event: Event::ResponseStarted(smith_events::ResponseStarted {
                run: 1,
                conversation: 0,
                response: 1,
                turn: 1,
                attempt: 0,
                window: 1,
                model: smith_events::Model {
                    account: None,
                    backend: smith_events::Backend::Direct,
                    endpoint: b"codex".as_slice().into(),
                    model: b"gpt-6-luna".as_slice().into(),
                    window_tokens: 272_000,
                    output_tokens: 32000,
                    effort: Some(b"low".as_slice().into()),
                },
                messages: 8,
                output_tokens: 32000,
                tool_choice: smith_events::ToolChoice::None,
                prompt: Some(smith_events::Prompt {
                    system: Some(b"native system".as_slice().into()),
                    tools: Some(List::with_capacity(0)),
                    messages,
                }),
            }),
        };
        let bound = PROMPT_SINK_CEILING;
        let token = Token::new(3);
        let mut requests = Queue::with_capacity(2);
        let meter = Meter::new();
        meter.start();
        let mut sinks = Sinks::new(&limits, Capture::Everything, 65536, skein_lib::Duration::from_secs(30));
        sinks.add(Destination::Trace, token, Delivery::Complete).expect("actual trace destination");
        assert!(sinks.takes());
        sinks.record(&record);
        assert!(!sinks.takes(), "the full prompt consumes its one ordinary observation credit");
        sinks.finish();
        let measured = meter.end();
        let mut peak = meter.check(measured, bound, &"retain maximal escaped native prompt");
        let mut sent = 0_usize;
        for _ in 0_u32..1000 {
            meter.start();
            sinks.down(Time::ZERO, &mut requests);
            let measured = meter.end();
            while let Some(request) = requests.pop() {
                match request {
                    io::Request::Stream { stream: owner, down: stream::Down::Demand { room, .. } } => {
                        assert_eq!(room, 65536);
                        sinks.up(Time::ZERO, io::Event::Stream { owner, up: stream::Up::Room });
                    }
                    io::Request::Stream { stream: _, down: stream::Down::Send(bytes) } => {
                        assert!(bytes.len() <= 65536);
                        sent += bytes.len();
                    }
                    io::Request::Close { entity } => sinks.up(Time::ZERO, io::Event::Closed { owner: entity }),
                    other => panic!("unexpected native writer request {other:?}"),
                }
            }
            peak = peak.max(meter.check(measured, bound, &"bounded prompt chunk"));
            if sinks.closed() {
                break;
            }
        }
        assert!(sinks.closed());
        assert!(sent > 12_000_000 && sent <= usize::try_from(limits.output).expect("output fits usize"));
        assert!(peak > 12_000_000, "the allocator reaches actual retained escaped output");
        println!("PROMPT_SINK_MEMORY peak={peak} bound={bound} sent={sent} output_cap={}", limits.output);
        drop(sinks);
        assert_eq!(meter.held(), 0, "all destination ownership releases");
    }

    #[test]
    #[expect(
        clippy::print_stdout,
        reason = "record independently measured peak and boundary bytes for the proof receipt"
    )]
    fn retained_escaped_calls_and_partial_sends_stay_within_destination_ownership() {
        // Peak 3_933_468 B on 2026-10-28; a quarter of headroom, rounded up.
        const CALL_SINK_CEILING: u64 = 5_000_000;

        let limits = SinkLimits {
            destinations: 2,
            output: 8 * 1024 * 1024,
            records: smith_events::Limits { string: 65536, content: 16384, items: 32 },
            answer_bytes: 16384,
            metadata_string: 64,
            metadata_items: 32,
            scalar_content: 16384,
            observation_line: 0,
            fact_extra: 0,
            conversations: 6,
        };
        let record = Record {
            t_ms: 1,
            event: Event::ToolStarted(ToolStarted {
                run: 1,
                conversation: 1,
                call: vec![0; 65536].into_boxed_slice(),
                tool: vec![0; 65536].into_boxed_slice(),
                source: Source::Workspace,
                effect: Effect::Read,
                deadline_ms: None,
                input_bytes: 0,
                input: None,
            }),
        };
        let bound = CALL_SINK_CEILING;
        let tokens = [Token::new(1), Token::new(2)];
        let mut requests = Queue::with_capacity(2);
        let meter = Meter::new();
        meter.start();
        let mut sinks = Sinks::new(&limits, Capture::Everything, 4096, skein_lib::Duration::from_secs(30));
        sinks.add(Destination::Trace, tokens[0], Delivery::Complete).expect("trace");
        sinks.add(Destination::Output, tokens[1], Delivery::Complete).expect("output");
        assert!(sinks.takes());
        // One gathered fact can project two records before its next credit check.
        sinks.record(&record);
        sinks.record(&record);
        assert!(!sinks.takes(), "slow destinations hold the next fact");
        sinks.finish();
        let measured = meter.end();
        let mut peak = meter.check(measured, bound, &"retain two owning lines per destination");
        let mut sent = [0_usize; 2];
        for _ in 0_u32..2000 {
            meter.start();
            sinks.down(Time::ZERO, &mut requests);
            let measured = meter.end();
            while let Some(request) = requests.pop() {
                match request {
                    io::Request::Stream { stream: owner, down: stream::Down::Demand { room, .. } } => {
                        assert_eq!(room, 4096);
                        sinks.up(Time::ZERO, io::Event::Stream { owner, up: stream::Up::Room });
                    }
                    io::Request::Stream { stream: owner, down: stream::Down::Send(bytes) } => {
                        assert!(bytes.len() <= 4096);
                        let index = tokens.iter().position(|token| *token == owner).expect("actual writer");
                        sent[index] += bytes.len();
                        drop(bytes);
                    }
                    io::Request::Close { entity } => sinks.up(Time::ZERO, io::Event::Closed { owner: entity }),
                    other => panic!("unexpected request {other:?}"),
                }
            }
            peak = peak.max(meter.check(measured, bound, &"bounded partial send"));
            if sinks.closed() {
                break;
            }
        }
        assert!(sinks.closed());
        assert_eq!(sent[0], sent[1]);
        assert!(sent[0] > 2 * 65536 * 6, "both escaped identities crossed both boundaries");
        assert!(peak > 2 * 65536 * 6, "meter reached retained encoded output");
        assert!(sinks.pop_given_up().is_none());
        println!("SINK_MEMORY peak={peak} bound={bound} sent_per_destination={}", sent[0]);
        drop(sinks);
        assert_eq!(meter.held(), 0, "all sink ownership released");
    }
}
