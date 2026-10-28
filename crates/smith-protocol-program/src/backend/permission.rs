//! Permission replies wait for the domain's next Complete (programs.md 5.7).
//! Held prompts share the MCP queue's capacity and payload bound. Correlation
//! uses the provider's tool id, or its name/input when the id is absent.

use super::{Backend, Phase, policy};
use crate::mcp::{self, json};
use alloc::boxed::Box;
use skein_lib::List;
use smith_domain::llm;
use smith_protocol_llm::FromDomain;

impl Backend {
    pub(super) fn permission(&mut self, prompt: mcp::Up) {
        let (call, tool_name, input, tool_use_id) = match prompt {
            mcp::Up::Prompt { call, tool_name, input, tool_use_id } => (call, tool_name, input, tool_use_id),
            mcp::Up::Call { .. } | mcp::Up::Connected | mcp::Up::Listening { .. } | mcp::Up::Ended(_) => {
                unreachable!("permission prompt")
            }
        };
        if self.phase == Phase::Ending || self.stop != super::stop::Stop::None {
            self.server.as_mut().expect("stopped permission prompt").verdict(
                call,
                mcp::Verdict::Deny { message: super::stop::INTERRUPTED.into() },
                &mut self.io,
            );
            return;
        }
        if self.native.is_some() {
            self.calls.push(mcp::Up::Prompt { call, tool_name, input, tool_use_id }).expect("bounded held prompts");
            return;
        }
        let Some(slot) = self.permission_index(&tool_name, &input, tool_use_id.as_deref()) else {
            self.calls.push(mcp::Up::Prompt { call, tool_name, input, tool_use_id }).expect("bounded held prompts");
            return;
        };
        let verdict = match &self.request {
            Some(FromDomain::Complete { prompt, .. }) => match &prompt.choice {
                llm::ToolChoice::Auto => {
                    policy::judge(&tool_name, &input, self.grants(), &self.mounts, self.settings.path_bytes)
                }
                llm::ToolChoice::Only(names) => mcp::Verdict::Deny { message: outside(names) },
                llm::ToolChoice::None => {
                    mcp::Verdict::Deny { message: b"not run: allowed calls now: none".as_slice().into() }
                }
            },
            Some(FromDomain::Cancel { .. } | FromDomain::Yield { .. }) | None => return,
        };
        if let mcp::Verdict::Allow { .. } = &verdict {
            let held = self.ledger.get_mut(slot).expect("permission ledger");
            held.native_write = tool_name.as_ref() == b"Write" || tool_name.as_ref() == b"Edit";
            if tool_name.as_ref() == b"Read"
                && super::native::whole(&input)
                && let Some(at) = super::native::target(&self.mounts, &input, self.settings.path_bytes)
            {
                self.native = Some(super::native::Snapshot::new(
                    held.owner.expect("permission follows completed call"),
                    at,
                    super::native::After::Allow { call, input },
                ));
                return;
            }
        }
        self.server.as_mut().expect("permission server").verdict(call, verdict, &mut self.io);
    }

    #[cfg(test)]
    pub(super) fn permission_ready(&mut self, tool_name: &[u8], input: &[u8], tool_use_id: Option<&[u8]>) -> bool {
        self.permission_index(tool_name, input, tool_use_id).is_some()
    }

    fn permission_index(&mut self, tool_name: &[u8], input: &[u8], tool_use_id: Option<&[u8]>) -> Option<u32> {
        for index in 0..self.ledger.len() {
            let held = self.ledger.get_mut(index).expect("ledger entry");
            let same = match tool_use_id {
                Some(id) => id == held.id.as_ref(),
                None => {
                    held.name.as_ref() == tool_name
                        && super::calls::equal(
                            &held.input,
                            input,
                            self.settings.adapter.client.depth,
                            self.settings.adapter.stated.call_input_bytes,
                        )
                }
            };
            if held.builtin && !held.answered && !held.prompted && same && held.message < self.permitted {
                held.prompted = true;
                return Some(index);
            }
        }
        None
    }
}

pub(super) fn outside(names: &[llm::Served]) -> Box<[u8]> {
    let mut bytes = List::with_capacity(4096);
    for byte in b"not run: allowed calls now: " {
        bytes.push(*byte).expect("prefix fits");
    }
    for tool in names {
        let name = match tool {
            llm::Served::Finish => b"finish".as_slice(),
            llm::Served::Deliver => b"deliver",
            llm::Served::Wait => b"wait",
            llm::Served::SubAgent => b"sub_agent",
            llm::Served::Host(tool) => &tool.name,
        };
        for byte in name {
            bytes.push(*byte).expect("finishing tool names fit");
        }
        bytes.push(b' ').expect("finishing names fit");
    }
    // The fixed finishing choice contains finish and, when granted, deliver.
    json::join(&[bytes.as_slice()])
}
