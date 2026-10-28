//! The run child domain's state and its entry points (domain/run.md, sections
//! 3, 5, 6 and 10; programming-model.md, sections 4.5 and 6.3). [`Domain`]
//! retains seeded admission randomness, runs, conversations, delegated calls,
//! alarms, deferred wind-down effects and bounded facts.
//! [`step`] accepts parent events, [`fire`] handles due alarms and [`resume`]
//! emits deferred child closures and main's yield after a reclaim point. All three emit
//! bounded requests. This module never knows provider syntax, credential
//! secrets, host policy or the contents of a delivered receipt.

use skein_lib::{Deadlines, Env, Queue, Rng, Slab, Time, Token};

use crate::boundary::{Event, Request};
use crate::call::Calls;
use crate::facts::{self, Fact, Facts};
use crate::limits::Limits;
use crate::run::{self, Alarm, Conversation, Run};

/// The most requests an entry point emits per call: an admitted start names
/// the run and opens its main conversation or asks io for its first look; a
/// check goes with its notice to the host; a call returns as main is
/// closed. The parent reserves this much room in `out` before calling it.
pub const MAX_OUT: u32 = 3;

/// A terminal may observe every offered and queued message, its end and
/// answer; other entrances observe no more. The parent reserves this room.
#[must_use]
pub const fn max_facts(limits: &Limits) -> u32 {
    limits.messages.saturating_mul(2).saturating_add(8)
}

/// The run child domain's state.
#[derive(Debug)]
pub struct Domain {
    pub(crate) rng: Rng,
    pub(crate) runs: Slab<Run>,
    pub(crate) conversations: Slab<Conversation>,
    /// Calls of conversations to the run.
    pub(crate) calls: Calls,
    pub(crate) alarms: Deadlines<Alarm>,
    pub(crate) facts: Facts,
    pub(crate) ready: crate::wind_down::Ready,
}

impl Domain {
    /// A domain with room for `limits`, drawing absent identities from `seed`.
    #[must_use]
    pub fn new(limits: &Limits, seed: u64) -> Domain {
        Domain {
            rng: Rng::new(seed),
            runs: Slab::with_capacity(limits.runs),
            conversations: Slab::with_capacity(limits.conversations),
            calls: Calls::with_capacity(limits.calls),
            alarms: Deadlines::with_capacity(crate::limits::alarms(limits).expect("valid limits")),
            facts: Facts::with_capacity(limits.facts),
            ready: crate::wind_down::Ready::with_capacity(limits.runs),
        }
    }

    /// Child closures that may go on after a reclaim point.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.ready.is_ready()
    }

    /// Runs present, closed ones included until they are reclaimed.
    #[must_use]
    pub fn runs(&self) -> u32 {
        self.runs.len()
    }

    /// Conversations present, ended ones included until they are reclaimed.
    #[must_use]
    pub fn conversations(&self) -> u32 {
        self.conversations.len()
    }

    /// When the earliest alarm falls due.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        self.alarms.next()
    }

    /// Whether an alarm is due at `now`. While one is, the loop fires the
    /// root domain, which calls [`fire`].
    #[must_use]
    pub fn is_due(&self, now: Time) -> bool {
        match self.alarms.next() {
            Some(at) => at <= now,
            None => false,
        }
    }

    /// Calls of conversations to the run, returned ones included until they
    /// are reclaimed.
    #[must_use]
    pub fn calls(&self) -> u32 {
        self.calls.len()
    }

    /// The oldest reserved observation; the parent drains it before more work.
    pub fn pop_fact(&mut self) -> Option<Fact> {
        self.facts.pop()
    }

    /// Free fact slots the parent reserves before each entrance.
    #[must_use]
    pub fn facts_room(&self) -> u32 {
        self.facts.room()
    }

    /// The reclaim point: frees what closed in this iteration.
    pub fn reclaim(&mut self) {
        self.runs.reclaim();
        self.conversations.reclaim();
        self.calls.reclaim();
        self.ready.promote();
    }
}

/// Handles one event, emitting at most [`MAX_OUT`] requests.
pub fn step(domain: &mut Domain, env: &Env<Limits>, event: Event, out: &mut Queue<Request>) {
    assert!(domain.facts_room() >= max_facts(&env.limits), "parent reserved fact step output");
    domain.facts.begin(env.now);
    let mark = out.len();
    take(domain, env, event, out);
    facts::tell(&mut domain.facts, &domain.runs, &domain.conversations, out, mark);
}

fn take(domain: &mut Domain, env: &Env<Limits>, event: Event, out: &mut Queue<Request>) {
    match event {
        Event::HostReturned { relay, reply } => run::host_returned(domain, env, relay, reply, out),
        Event::Start {
            reply_to,
            host_run,
            activation,
            identity,
            window,
            messages,
            charter,
            workspace,
            transcript,
            resumed,
        } => {
            run::start(
                domain,
                env,
                run::Start {
                    reply_to,
                    host_run,
                    activation,
                    identity,
                    window,
                    messages,
                    charter,
                    workspace,
                    transcript,
                    resumed,
                },
                out,
            );
        }
        Event::Acknowledge { run, turn } => run::acknowledge(domain, run, turn),
        Event::Message { finish_requested, run, name, label, text } => {
            run::message(domain, env, run, crate::Message { finish_requested, name, label, text }, out);
        }
        Event::Turn { conversation, record, sequence, opens_window } => {
            run::turn(domain, conversation, record, sequence, opens_window, out);
        }
        Event::Cancel { run } => run::cancel(domain, run, out),
        Event::Started { conversation, peer } => run::started(domain, conversation, peer, out),
        Event::JobsCancelled { conversation } => run::jobs_cancelled(domain, env, conversation, out),
        Event::Yielded { conversation, stop, text } => run::yielded(domain, env, conversation, stop, &text, out),
        Event::Priced { conversation, own_spent, subtree_spent } => {
            run::priced(domain, conversation, own_spent, subtree_spent, out);
        }
        Event::Used { conversation, spend } => run::used(domain, conversation, spend, out),
        Event::Ended { conversation, end, spend, detail } => run::ended(domain, conversation, end, spend, detail, out),
        Event::Read { owner, read } => run::read(domain, env, owner, read, out),
        Event::Probed { owner, executable } => run::probed(domain, env, owner, executable, out),
        Event::Delegated { conversation, call, name, ask, deadline } => {
            run::delegated(domain, env, conversation, call, name, ask, deadline, out);
        }
        Event::Withdraw { conversation, call } => run::withdraw(domain, conversation, call, out),
        Event::Checked { owner, ran } => run::checked(domain, env, owner, ran, out),
        Event::Aborted { owner } => run::aborted(domain, owner, out),
        Event::Delivered { owner, delivery } => run::delivered(domain, owner, delivery, out),
    }
}

/// Fires the earliest alarm due at `env.now`, if there is one, emitting at most
/// [`MAX_OUT`] requests. A stage fires its alarms after its input events, so
/// progress that arrived in the same iteration wins over a deadline that passed
/// while the loop waited.
pub fn fire(domain: &mut Domain, env: &Env<Limits>, out: &mut Queue<Request>) {
    assert!(domain.facts_room() >= max_facts(&env.limits), "parent reserved fact step output");
    let Some(alarm) = domain.alarms.expire(env.now) else {
        return;
    };
    domain.facts.begin(env.now);
    let mark = out.len();
    match alarm {
        Alarm::Deadline { run } => run::deadline(domain, run, out),
        Alarm::Park { run } => run::park(domain, run, out),
        Alarm::Call { call } => run::expired(domain, call, out),
        Alarm::Host { call } => run::host_alarm(domain, env, call, out),
    }
    facts::tell(&mut domain.facts, &domain.runs, &domain.conversations, out, mark);
}

/// Emits one deferred child close, preserving the entrance's fixed output bound.
pub fn resume(domain: &mut Domain, env: &Env<Limits>, out: &mut Queue<Request>) {
    assert!(domain.facts_room() >= max_facts(&env.limits), "parent reserved fact resume output");
    domain.facts.begin(env.now);
    let mark = out.len();
    run::resume(domain, out);
    facts::tell(&mut domain.facts, &domain.runs, &domain.conversations, out, mark);
}

/// Root asks before publishing a completion. This may record the finishing
/// stage and keep deferred child closures; it never starts provider work.
/// `most` is its priced maximum, or Unreserved for a program (programs.md 5.7).
#[must_use]
pub fn completion_permit(
    domain: &mut Domain,
    conversation: Token,
    now: Time,
    most: crate::Most,
) -> crate::CompletionPermit {
    run::completion_permit(domain, conversation, now, most)
}

/// Admit a completion, reserving a direct backend's priced maximum (programs.md 5.7).
pub fn reserve(
    domain: &mut Domain,
    conversation: Token,
    owner: Token,
    most: crate::Most,
) -> Result<(), crate::Exhausted> {
    run::reserve(domain, conversation, owner, most)
}

/// Settle one provider terminal, returning whether its charge obeyed its reservation.
pub fn settle_reservation(domain: &mut Domain, conversation: Token, owner: Token, charge: u64) -> bool {
    run::settle_reservation(domain, conversation, owner, charge)
}

/// Live run that owns this conversation, for its parent's acknowledgement routing.
#[must_use]
pub fn owner(domain: &Domain, conversation: Token) -> Option<Token> {
    let conversation = domain.conversations.get(skein_lib::Id::from_token(conversation))?;
    Some(conversation.run.token())
}

/// The charter of the live run that owns this conversation.
#[must_use]
pub fn conversation_charter(domain: &Domain, conversation: Token) -> Option<&crate::Charter> {
    let conversation = domain.conversations.get(skein_lib::Id::from_token(conversation))?;
    Some(&domain.runs.get(conversation.run)?.charter)
}

/// The host's logical name for the run owning a live conversation.
#[must_use]
pub fn host_name(domain: &Domain, conversation: Token) -> Option<Token> {
    let conversation = domain.conversations.get(skein_lib::Id::from_token(conversation))?;
    Some(domain.runs.get(conversation.run)?.host_name)
}

/// Check one provider completion against exact run-wide arithmetic before its
/// calls enter the session.
#[must_use]
pub fn completion_overflow(
    domain: &Domain,
    conversation: Token,
    price: u64,
    usage: crate::Spend,
) -> Option<crate::Failure> {
    run::completion_overflow(domain, conversation, price, usage)
}
