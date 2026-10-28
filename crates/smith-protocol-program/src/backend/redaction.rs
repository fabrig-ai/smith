//! Partial text keeps only a possible private-pattern prefix per open block.
//! Carries flush at stream and lifecycle boundaries (programs.md 9; events.md 3.5).
use super::Backend;
use alloc::boxed::Box;
use skein_lib::{List, Time, Token};
use smith_protocol_llm::Measure;

pub(super) struct Carry {
    owner: Option<Token>,
    bytes: List<u8>,
    redacted: bool,
}

pub(super) fn capacity(settings: &super::Settings) -> Option<u32> {
    directory_capacity(settings.program.directory.len())
}

fn directory_capacity(length: usize) -> Option<u32> {
    // directory + optional slash + .runs/<32 hex bytes>/instructions.md.
    u32::try_from(length).ok()?.checked_add(55)?.max(64).checked_sub(1)
}

pub(super) fn carries(settings: &super::Settings) -> List<Carry> {
    let mut carries = List::with_capacity(settings.receiving.max_completion_blocks);
    for _ in 0..settings.receiving.max_completion_blocks {
        assert!(
            carries.push(Carry { owner: None, bytes: List::with_capacity(0), redacted: false }).is_ok(),
            "block slots"
        );
    }
    carries
}

impl Backend {
    pub(super) fn text_delta(&mut self, at: Time, owner: Token, block: u32, text: &[u8]) -> Option<()> {
        let carry = self.delta_carry.get_mut(block)?;
        let old = core::mem::replace(&mut carry.bytes, List::with_capacity(0));
        let was_redacted = carry.redacted;
        let length = old.len().checked_add(u32::try_from(text.len()).ok()?)?;
        let mut joined = List::with_capacity(length);
        for byte in old.as_slice().iter().chain(text) {
            joined.push(*byte).ok()?;
        }
        let suffix = self.private_suffix(joined.as_slice());
        let emitted = joined.len().checked_sub(suffix)?;
        let carry = self.delta_carry.get_mut(block)?;
        carry.owner = Some(owner);
        carry.bytes = List::with_capacity(capacity(&self.settings)?);
        for byte in joined.as_slice().get(usize::try_from(emitted).ok()?..)? {
            carry.bytes.push(*byte).ok()?;
        }
        let mut joined = joined.into_boxed();
        self.redact_private(&mut joined);
        if was_redacted {
            for byte in joined.get_mut(..usize::try_from(old.len()).ok()?)? {
                *byte = b'*';
            }
        }
        let carry = self.delta_carry.get_mut(block)?;
        carry.redacted = false;
        for (raw, hidden) in carry.bytes.as_slice().iter().zip(joined.get(usize::try_from(emitted).ok()?..)?) {
            if raw != hidden {
                carry.redacted = true;
            }
        }
        if emitted > 0 {
            let text = Box::from(joined.get(..usize::try_from(emitted).ok()?)?);
            self.measures.push(Measure::Delta { at, owner, block, text });
        }
        Some(())
    }

    pub(super) fn flush_delta(&mut self, at: Time, block: u32) {
        let Some(carry) = self.delta_carry.get_mut(block) else { return };
        let owner = carry.owner.take();
        let was_redacted = carry.redacted;
        carry.redacted = false;
        let mut text = core::mem::replace(&mut carry.bytes, List::with_capacity(0)).into_boxed();
        if !text.is_empty()
            && let Some(owner) = owner
        {
            self.redact_private(&mut text);
            if was_redacted {
                text.fill(b'*');
            }
            self.measures.push(Measure::Delta { at, owner, block, text });
        }
    }

    pub(super) fn flush_deltas(&mut self, at: Time) {
        for block in 0..self.delta_carry.len() {
            self.flush_delta(at, block);
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn invalid_limits_are_refused() {
        assert_eq!(super::directory_capacity(0), Some(63));
        assert!(super::directory_capacity(usize::try_from(u32::MAX - 54).unwrap()).is_none());
        assert!(super::directory_capacity(usize::MAX).is_none());
    }
}
