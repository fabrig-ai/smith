//! Random startup declarations preserve capacities and named refusals.

#![cfg(test)]
#![allow(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::unwrap_used,
    clippy::missing_assert_message,
    reason = "programming-model.md section 10.2: deterministic test fixtures use ordinary Rust"
)]

use smith_agent_service as service;
use smith_agent_service::sinks::Capture;
use smith_agent_shell::config;

#[path = "support/product.rs"]
mod product;

use product::product_document;

fn trust_path() -> String {
    skein_scratch::paths::workspace().join("tests/protocol-llm/fixtures/root.der").display().to_string()
}

fn parse_value(document: &serde_json::Value) -> Result<config::Configuration, String> {
    config::parse(&serde_json::to_vec(document).expect("drawn document"))
}

#[test]
fn drawn_declarations_preserve_capacities_or_name_their_refusal() {
    use service::profile::validate;
    use skein_lib::Rng;

    let mut base = product_document();
    base["endpoints"][0]["trust_der"] = trust_path().into();
    let baseline = config::from_value(base).expect("bounded product startup").declarations;
    assert_eq!(baseline.environment_bytes, 835);
    let mut rng = Rng::new(4_170_005);
    let (mut admitted, mut relationship_refusals) = (0, 0);
    for sample in 0..96 {
        let mut document = product_document();
        // Native endpoint construction uses the bounded DER loader and test root.
        document["endpoints"][0]["trust_der"] = trust_path().into();
        let payload = rng.next_u64() % 40_960 + 1;
        let declared = &mut document["profile"]["declared"];
        declared["conversations"] = (rng.next_u64() % 6 + 1).into();
        declared["tool_payload"] = payload.into();
        declared["file_bytes"] = (rng.next_u64() % 1_048_576 + payload).into();
        declared["calls_per_response"] = (rng.next_u64() % 16 + 1).into();
        declared["read_window"] =
            if sample % 6 == 0 { (payload + 1).into() } else { (rng.next_u64() % payload + 1).into() };
        // Positive memory declarations are carried independently of the
        // capacities and relationships drawn here.
        declared["memory"] = if sample % 6 == 1 { 1_u64.into() } else { 2_147_483_648_u64.into() };
        let model = &mut document["endpoints"][0]["models"][0];
        let window = rng.next_u64() % 272_000 + 272_000;
        let output = rng.next_u64() % 32_000 + 32_000;
        model["window"] = window.into();
        model["output"] = output.into();
        let profile = config::profile_from_value(document["profile"].clone()).expect("positive drawn declarations");
        let mut declarations = baseline.clone();
        declarations.endpoints[0].models[0].window = u32::try_from(window).unwrap();
        declarations.endpoints[0].models[0].output = u32::try_from(output).unwrap();
        let limits = match service::profile::derive(&profile, &declarations) {
            Ok(limits) => limits,
            Err(refusal) => {
                let check = refusal.check().expect("draw names a startup relationship");
                let words = String::from_utf8(service::profile::words(&refusal).into_vec()).unwrap();
                assert!(words.starts_with(check.name()), "{words}");
                assert_eq!(parse_value(&document).err().as_deref(), Some(words.as_str()));
                relationship_refusals += 1;
                continue;
            }
        };
        validate::declarations(&profile, &declarations).expect("all drawn declaration comparisons");
        validate::window_first(&profile, &declarations, &limits.domain.session).expect("whole window and batch");
        validate::machine(&limits.machine, &limits.domain).expect("machine holds tools");
        assert!(limits.file_bytes >= limits.domain.session.tools.file_bytes);
        let effort = smith_charter::CEILINGS.llm_effort_some;
        service::EventLimits::derive(&limits, Capture::Everything, 1, effort, &[], profile.policy.write_deadline)
            .expect("drawn event ownership");
        let configured = config::from_value(document.clone()).expect("drawn native endpoints");
        assert_eq!(configured.declarations, declarations, "the independent drawn declarations match startup");
        assert_eq!(configured.memory, profile.declared.memory);
        parse_value(&document).expect("all concrete startup validity checks");
        admitted += 1;
    }
    assert!(admitted > 0 && relationship_refusals > 0);
    println!("DRAWN_STARTUP samples=96 admitted={admitted} relationship_refusals={relationship_refusals}");
}
