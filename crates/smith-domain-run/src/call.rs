//! The calls a conversation's LLM makes of the run, its delegated tools
//! (domain/run.md, sections 5.2, 5.3, 6 and 8.4): each
//! an entity from its `Delegated` to its one `Return`, under the `calls`
//! limit, so that a call's slot never waits on the reclaim point.
//!
//! A call is named by its conversation's token for it, which the run cannot
//! echo back (no record carries the run's name for it to the session), so a
//! bounded map finds a call by its conversation and that name, for a
//! `Withdraw`.
//!
//! Each call has a deadline, its conversation's own expiry, and the run runs
//! the race (programming-model.md, section 5.3): an alarm per call, under the same limit. Past it, the run
//! stops what the call is doing, and returns it as timed out once that has
//! settled, so a call never returns with anything it started still in flight.
//! A conversation withdraws a call only as it closes, and a call its deadline
//! stopped first stays stopped for that.

use skein_lib::{Id, Map, Slab, Token};

use crate::agent::Child;
use crate::boundary::Returned;
use crate::land::Landing;
use crate::run::{Conversation, Run};

/// A call in flight.
#[derive(Debug)]
pub(crate) struct Call {
    pub(crate) run: Id<Run>,
    /// The conversation that made it.
    pub(crate) conversation: Id<Conversation>,
    /// The conversation's token for it.
    pub(crate) owner: Token,
    pub(crate) work: Work,
}

/// What a call is doing.
#[derive(Debug)]
pub(crate) enum Work {
    Host(crate::host::Relay),
    /// A finish, landing its change.
    Landing(Landing),
    /// A sub-agent's.
    Child(Child),
}

/// Why a call is stopped before it is done.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Withdrawal {
    /// Its conversation withdrew it, closing.
    Withdrawn,
    /// Its deadline passed first.
    Expired,
}

/// What a call stopped for `why` returns, once what it was doing has settled.
pub(crate) fn stopped(why: Withdrawal) -> Returned {
    match why {
        Withdrawal::Withdrawn => Returned::Cancelled,
        Withdrawal::Expired => Returned::TimedOut,
    }
}

/// The calls in flight, and the names their conversations know them by.
#[derive(Debug)]
pub(crate) struct Calls {
    slab: Slab<Call>,
    named: Map<(Id<Conversation>, Token), Id<Call>>,
}

impl Calls {
    pub(crate) fn with_capacity(capacity: u32) -> Calls {
        Calls { slab: Slab::with_capacity(capacity), named: Map::with_capacity(capacity) }
    }

    /// Calls present, returned ones included until they are reclaimed.
    pub(crate) fn len(&self) -> u32 {
        self.slab.len()
    }

    /// Whether another call would be refused.
    pub(crate) fn is_full(&self) -> bool {
        self.slab.is_full() || self.named.len() >= self.named.capacity()
    }

    /// Stores `call` and names it; there must be room.
    pub(crate) fn insert(&mut self, call: Call) -> Id<Call> {
        let name = (call.conversation, call.owner);
        let id = self.slab.insert(call).expect("checked for room before a call is made");
        let fresh = self.named.insert(name, id).expect("as much room for names as for calls");
        assert!(fresh.is_none(), "a conversation names its calls apart");
        id
    }

    pub(crate) fn get(&self, id: Id<Call>) -> Option<&Call> {
        self.slab.get(id)
    }

    pub(crate) fn get_mut(&mut self, id: Id<Call>) -> Option<&mut Call> {
        self.slab.get_mut(id)
    }

    /// The call `conversation` names `owner`, if it has not returned.
    pub(crate) fn find(&self, conversation: Id<Conversation>, owner: Token) -> Option<Id<Call>> {
        self.named.get(&(conversation, owner)).copied()
    }

    /// The sole write waiting for this conversation's jobs to settle.
    pub(crate) fn exclusive(&self, conversation: Id<Conversation>) -> Option<Id<Call>> {
        let mut found = None;
        for (name, id) in &self.named {
            if name.0 == conversation {
                assert!(found.is_none(), "a job cancellation belongs to one exclusive call");
                found = Some(*id);
            }
        }
        found
    }

    /// Retires a call that has returned: its name is forgotten at once, its
    /// slot freed at the reclaim point.
    pub(crate) fn retire(&mut self, id: Id<Call>) {
        let call = self.slab.get(id).expect("a call lives until it returns");
        let named = self.named.remove(&(call.conversation, call.owner));
        assert!(named == Some(id), "a call is named until it returns");
        self.slab.retire(id);
    }

    pub(crate) fn reclaim(&mut self) {
        self.slab.reclaim();
    }
}
