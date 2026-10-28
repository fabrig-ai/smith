use super::{parse_value, shaped_document};
use crate::config::{Program, ProgramName, from_value};
use smith_agent_service::{program_operations, program_worst_case, worst_case};

fn document() -> serde_json::Value {
    let mut document = shaped_document();
    document["programs"] = serde_json::json!({"claude_code": {
        "path": "/usr/local/bin/claude", "directory": "/home/u/.local/state/smith/programs"
    }});
    document
}

#[test]
fn an_unbound_program_has_no_backend_charge() {
    let direct = from_value(shaped_document()).expect("direct configuration");
    let declared = from_value(document()).expect("unbound declaration");
    let direct_complete = worst_case(&direct.service.limits, &direct.service.llm_endpoints).unwrap()
        + program_worst_case(&direct.service).unwrap();
    let declared_complete = worst_case(&declared.service.limits, &declared.service.llm_endpoints).unwrap()
        + program_worst_case(&declared.service).unwrap();
    println!("UNBOUND direct={direct_complete} declared={declared_complete}");
    assert_eq!(program_worst_case(&declared.service), Some(0));
    assert_eq!(program_operations(&declared.service), Some(declared.service.limits.routes));
    assert_eq!(declared_complete, direct_complete);
}

#[test]
fn an_unbound_program_root_is_closed_by_the_shell() {
    use skein_io::kernel::{Complete, Done, Fd, Op};
    use skein_lib::{Secret, Time, Wall};
    use skein_shell::Host;

    let configuration = from_value(document()).expect("unbound declaration");
    let mut agent = crate::Agent::new(
        configuration,
        crate::Resources {
            input: Fd::new(0),
            output: Fd::new(1),
            signals: Fd::new(2),
            seed: 1,
            pid: 1,
            trace: None,
            error: None,
            roots: None,
            programs: Some(crate::ProgramResources {
                relay: "/smith-agent".into(),
                entropy: Secret::new(Box::new([7; 32])),
                directory: Some(Fd::new(3)),
            }),
        },
        Box::new(std::io::sink()),
    )
    .expect("unbound declaration starts with shell resources");
    let close = agent.submissions().pop().expect("unused root closes immediately");
    assert_eq!(close.kind, Op::Close { fd: Fd::new(3) });
    assert!(agent.submissions().is_empty());
    agent.completions().push(Complete { op: close.op, kind: close.kind, result: Ok(Done::Nothing) });
    agent.iterate(Time::ZERO, Wall::EPOCH);
    assert!(!agent.submissions().iter().any(|submit| matches!(submit.kind, Op::Close { fd } if fd == Fd::new(3))));
}

#[test]
fn a_configuration_names_claude_code_s_paths() {
    let configuration = parse_value(&document()).expect("program configuration");
    assert_eq!(configuration.programs.len(), 1);
    assert_eq!(
        configuration.programs.get(&ProgramName::ClaudeCode),
        Some(&Program {
            path: "/usr/local/bin/claude".into(),
            directory: "/home/u/.local/state/smith/programs".into(),
        })
    );
    assert!(configuration.endpoint_programs.is_empty(), "only endpoints naming a program are stored");
}

#[test]
fn an_endpoint_naming_a_program_reads_it() {
    let mut document = document();
    document["endpoints"][0]["program"] = "claude_code".into();
    let configuration = parse_value(&document).expect("endpoint program");
    assert_eq!(configuration.endpoint_programs.get(&0), Some(&ProgramName::ClaudeCode));
    assert!(
        configuration.service.domain.valid(&configuration.service.limits.domain),
        "program declarations fit the startup pool"
    );
}

#[test]
fn an_endpoint_naming_a_program_without_a_path_is_refused() {
    let mut document = shaped_document();
    document["endpoints"][0]["program"] = "claude_code".into();
    let error = parse_value(&document).err().expect("undeclared program");
    assert!(error.contains("program[endpoint=0]"), "endpoint program field: {error}");
}

#[test]
fn a_relative_program_path_is_refused() {
    for field in ["path", "directory"] {
        let mut document = document();
        document["programs"]["claude_code"][field] = "relative".into();
        let error = parse_value(&document).err().expect("relative path");
        assert!(error.contains(&format!("programs.claude_code.{field}")), "path field: {error}");
    }
}

#[test]
fn an_unknown_program_is_refused() {
    let mut document = document();
    document["programs"]["other"] = document["programs"]["claude_code"].clone();
    let error = parse_value(&document).err().expect("unknown program");
    assert!(error.contains("programs.other"), "unknown field: {error}");
    let mut document = self::document();
    document["endpoints"][0]["program"] = "other".into();
    let error = parse_value(&document).err().expect("unknown endpoint program");
    assert!(error.contains("endpoints[0].program"), "endpoint field: {error}");
}

#[test]
fn program_paths_obey_the_byte_bound() {
    for field in ["path", "directory"] {
        for (path, accepted) in [
            (format!("/{}", "a".repeat(4095)), true),
            (format!("/{}", "a".repeat(4096)), false),
            ("/a\0b".into(), false),
        ] {
            let mut document = document();
            document["programs"]["claude_code"][field] = path.into();
            assert_eq!(parse_value(&document).is_ok(), accepted, "{field} bound");
        }
    }
}

#[test]
fn program_fields_are_required_strictly() {
    for field in ["path", "directory"] {
        let mut document = document();
        document["programs"]["claude_code"].as_object_mut().expect("program object").remove(field);
        let error = parse_value(&document).err().expect("required program field");
        assert!(error.contains(&format!("programs.claude_code.{field}")), "missing field: {error}");
    }
    let mut document = document();
    document["programs"]["claude_code"]["extra"] = true.into();
    let error = parse_value(&document).err().expect("unknown program field");
    assert!(error.contains("extra"), "unknown field: {error}");
}

#[test]
fn the_program_startup_pool_prices_declarations_independently() {
    let direct = parse_value(&shaped_document()).unwrap();
    let declared = parse_value(&document()).unwrap();
    let model_only = smith_agent_service::profile::derive(&direct.profile, &direct.declarations)
        .unwrap()
        .domain
        .configured_model_bytes;
    assert_eq!(direct.service.limits.domain.configured_model_bytes, model_only);
    assert_eq!(declared.service.limits.domain.configured_model_bytes, model_only);
    assert!(direct.service.program.is_none());
    assert!(declared.service.program.as_ref().unwrap().endpoints.is_empty());
    assert!(direct.service.domain.programs.is_empty());
    assert!(declared.service.domain.programs.is_empty());
    assert_eq!(direct.endpoint_programs, declared.endpoint_programs);
    let mut selected = document();
    selected["endpoints"][0]["program"] = "claude_code".into();
    let program = parse_value(&selected).unwrap();
    let declaration_cells = program.service.domain.programs.len() * size_of::<smith_domain::Program>();
    let name_bytes = program.service.domain.programs.len() * b"claude_code".len();
    let expected = model_only + u64::try_from(declaration_cells + name_bytes).unwrap();
    assert_eq!(program.service.limits.domain.configured_model_bytes, expected);
    assert_eq!(program.service.domain.programs.len(), 1);
    assert!(program.service.domain.valid(&program.service.limits.domain));
}
