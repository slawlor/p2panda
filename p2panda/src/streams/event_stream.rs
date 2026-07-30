// SPDX-License-Identifier: MIT OR Apache-2.0

use std::pin::Pin;

use futures_util::Stream;
use futures_util::stream::{SelectAll, StreamExt};
use p2panda_net::discovery::DiscoveryEvent;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;

use crate::authoriser::AuthoriserEvent;

/// System event.
///
/// System events encompass all network-related events which are not directly associated with a
/// topic.
#[derive(Clone, Debug, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum SystemEvent {
    Authoriser(AuthoriserEvent),
    Discovery(DiscoveryEvent),
}

pub type EventStream = Pin<Box<dyn Stream<Item = SystemEvent> + Send + Unpin + 'static>>;

/// Merge the provided event streams into a single, unified system event stream.
pub(crate) fn event_stream(
    authoriser_events: broadcast::Receiver<AuthoriserEvent>,
    discovery_events: broadcast::Receiver<DiscoveryEvent>,
) -> EventStream {
    let authoriser_broadcast_stream = BroadcastStream::new(authoriser_events);
    let discovery_broadcast_stream = BroadcastStream::new(discovery_events);

    let authoriser_stream = Box::pin(
        authoriser_broadcast_stream
            .filter_map(|event| async { event.ok().map(SystemEvent::Authoriser) })
            .boxed(),
    );

    let discovery_stream = Box::pin(
        discovery_broadcast_stream
            .filter_map(|event| async { event.ok().map(SystemEvent::Discovery) })
            .boxed(),
    );

    let mut stream_set = SelectAll::new();
    stream_set.push(authoriser_stream);
    stream_set.push(discovery_stream);

    Box::pin(stream_set)
}
