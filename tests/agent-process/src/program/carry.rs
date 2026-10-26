//! Host-owned window transfers; the service receives only ordinary wire frames.
use skein_lib::{Reader, Writer};

/// Extract the transcript bodies the agent gave its host, without inspecting domains.
#[must_use]
pub fn from_frames(frames: &[skein_fake_channel::Observed]) -> Vec<Box<[u8]>> {
    frames
        .iter()
        .filter(|frame| frame.kind == 0x0109)
        .map(|frame| {
            let turn = smith_channel::Turn::decode(&smith_channel::CEILINGS, &mut Reader::new(&frame.body))
                .expect("turn envelope");
            Box::from(turn.body())
        })
        .collect()
}

/// Supply a saved window with the next run's charter and grant.
#[must_use]
pub fn start_with_window(
    charter: &[u8],
    value: &smith_channel::GrantValue,
    turns: &[Box<[u8]>],
) -> skein_channel::Frame {
    let message = super::waiting::initial(true);
    let frame = crate::start_workspace_message(charter, 8, false, message.as_ref(), value, None);
    let mut start = smith_channel::Start::decode(&smith_channel::CEILINGS, &mut Reader::new(&frame.bytes()[8..]))
        .expect("start")
        .into_parts();
    start.transcript = smith_protocol_channel::transcript_piece(turns).expect("saved window");
    let start = smith_channel::Start::new(&smith_channel::CEILINGS, start).expect("restored start");
    let mut body = Writer::new(start.measure() as usize);
    start.encode(&mut body).expect("start body");
    let mut frame = skein_channel::frame_writer(0x0100, start.measure()).expect("frame");
    frame.put(&body.finish()).expect("frame body");
    frame.finish().expect("frame")
}
