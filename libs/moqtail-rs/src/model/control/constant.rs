// Copyright 2025 The MOQtail Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use core::convert::From;

use crate::model::error::ParseError;

/// The ALPN protocols this crate speaks, in order of preference (§6.2). An endpoint
/// that chooses picks the first one its peer also offers, so draft-22 is used whenever
/// both ends support it.
pub const SUPPORTED_VERSIONS: &str = "moqt-22,moqt-20";

/// A MOQT version this crate speaks, as negotiated by ALPN (§6.2).
///
/// The version is fixed by the handshake, never inferred from the bytes. Draft-20
/// and draft-22 differ on the wire only in LOCATION_FILTER (§9.20.9): draft-20
/// length-prefixes it and infers its form from the number of fields, draft-22
/// writes a Location Filter Type and no length. Everything else is shared.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Version {
  Draft20,
  #[default]
  Draft22,
}

impl Version {
  /// Every supported version, in order of preference.
  pub const ALL: [Version; 2] = [Version::Draft22, Version::Draft20];

  /// The ALPN token, e.g. `moqt-22`.
  pub const fn alpn(self) -> &'static str {
    match self {
      Version::Draft20 => "moqt-20",
      Version::Draft22 => "moqt-22",
    }
  }

  /// The version an ALPN token names, or `None` if this crate does not speak it.
  pub fn from_alpn(alpn: &[u8]) -> Option<Version> {
    Version::ALL
      .into_iter()
      .find(|v| v.alpn().as_bytes() == alpn)
  }
}

/// Control message types.
///
/// Reserved codepoints are absent from this enum so `TryFrom` rejects them; an endpoint
/// receiving an unknown message type MUST close the session. That includes `0x1E`,
/// PUBLISH_OK in version <= 17: draft-22 Table 5 reserves it, and PUBLISH is answered by
/// REQUEST_OK (§9.3), so accepting it would let a draft-17 PUBLISH_OK pass for a REQUEST_OK.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum ControlMessageType {
  Setup = 0x2F00,            // Control
  GoAway = 0x10,             // Control, Request
  Subscribe = 0x03,          // Request, First
  SubscribeOk = 0x04,        // Request
  RequestError = 0x05,       // Request
  RequestUpdate = 0x02,      // Request
  Fetch = 0x16,              // Request, First
  FetchOk = 0x18,            // Request
  TrackStatus = 0x0D,        // Request, First
  PublishNamespace = 0x06,   // Request, First
  RequestOk = 0x07,          // Request
  Namespace = 0x08,          // Request
  NamespaceDone = 0x0E,      // Request
  SubscribeNamespace = 0x50, // Request, First
  SubscribeTracks = 0x51,    // Request, First
  Publish = 0x1D,            // Request, First
  PublishDone = 0x0B,        // Request
  PublishSkipped = 0x0F,     // Request
  PublishStateNotify = 0x22, // Request
}

impl TryFrom<u64> for ControlMessageType {
  type Error = ParseError;

  fn try_from(value: u64) -> Result<Self, Self::Error> {
    match value {
      0x2F00 => Ok(ControlMessageType::Setup),
      0x10 => Ok(ControlMessageType::GoAway),
      0x03 => Ok(ControlMessageType::Subscribe),
      0x04 => Ok(ControlMessageType::SubscribeOk),
      0x05 => Ok(ControlMessageType::RequestError),
      0x02 => Ok(ControlMessageType::RequestUpdate),
      0x0B => Ok(ControlMessageType::PublishDone),
      0x16 => Ok(ControlMessageType::Fetch),
      0x18 => Ok(ControlMessageType::FetchOk),
      0x0D => Ok(ControlMessageType::TrackStatus),
      0x06 => Ok(ControlMessageType::PublishNamespace),
      0x07 => Ok(ControlMessageType::RequestOk),
      0x08 => Ok(ControlMessageType::Namespace),
      0x0E => Ok(ControlMessageType::NamespaceDone),
      0x50 => Ok(ControlMessageType::SubscribeNamespace),
      0x51 => Ok(ControlMessageType::SubscribeTracks),
      0x1D => Ok(ControlMessageType::Publish),
      0x0F => Ok(ControlMessageType::PublishSkipped),
      0x22 => Ok(ControlMessageType::PublishStateNotify),
      _ => Err(ParseError::InvalidType {
        context: " ControlMessageType::try_from(u64)",
        details: format!("Invalid type, got {value}"),
      }),
    }
  }
}

impl From<ControlMessageType> for u64 {
  fn from(value: ControlMessageType) -> Self {
    value as u64
  }
}

impl ControlMessageType {
  /// True if this message type MUST be the first on a new bidirectional request
  /// stream. A request stream that opens with any other type is a protocol
  /// violation and MUST be reset.
  pub fn is_first(self) -> bool {
    matches!(
      self,
      Self::Subscribe
        | Self::Fetch
        | Self::TrackStatus
        | Self::PublishNamespace
        | Self::SubscribeNamespace
        | Self::SubscribeTracks
        | Self::Publish
    )
  }
}

/// Which fields of a LOCATION_FILTER follow its type, and how they are read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum LocationFilterType {
  None = 0x0,
  RelativeStart = 0x1,
  AbsoluteStart = 0x2,
  AbsoluteStartGroupEnd = 0x3,
  AbsoluteRange = 0x4,
  NextObject = 0x5,
}

impl TryFrom<u64> for LocationFilterType {
  type Error = ParseError;

  fn try_from(value: u64) -> Result<Self, Self::Error> {
    match value {
      0x0 => Ok(LocationFilterType::None),
      0x1 => Ok(LocationFilterType::RelativeStart),
      0x2 => Ok(LocationFilterType::AbsoluteStart),
      0x3 => Ok(LocationFilterType::AbsoluteStartGroupEnd),
      0x4 => Ok(LocationFilterType::AbsoluteRange),
      0x5 => Ok(LocationFilterType::NextObject),
      _ => Err(ParseError::InvalidType {
        context: "LocationFilterType::try_from(u64)",
        details: format!("Invalid type, got {value}"),
      }),
    }
  }
}

impl From<LocationFilterType> for u64 {
  fn from(value: LocationFilterType) -> Self {
    value as u64
  }
}

/// How a switch stops delivery on the subscription it suspends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum SwitchMode {
  /// Forward State goes to 0 and the subscription's open streams are reset.
  Hard = 0x0,
  Soft = 0x1,
}

impl TryFrom<u64> for SwitchMode {
  type Error = ParseError;

  fn try_from(value: u64) -> Result<Self, Self::Error> {
    match value {
      0x0 => Ok(SwitchMode::Hard),
      0x1 => Ok(SwitchMode::Soft),
      _ => Err(ParseError::InvalidType {
        context: "SwitchMode::try_from(u64)",
        details: format!("Invalid mode, got {value}"),
      }),
    }
  }
}

impl From<SwitchMode> for u64 {
  fn from(value: SwitchMode) -> Self {
    value as u64
  }
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum GroupOrder {
  Original = 0x0,
  Ascending = 0x1,
  Descending = 0x2,
}

impl TryFrom<u8> for GroupOrder {
  type Error = ParseError;

  fn try_from(value: u8) -> Result<Self, Self::Error> {
    match value {
      0x0 => Ok(GroupOrder::Original),
      0x1 => Ok(GroupOrder::Ascending),
      0x2 => Ok(GroupOrder::Descending),
      _ => Err(ParseError::InvalidType {
        context: "GroupOrder::try_from(u8)",
        details: format!("Invalid type, got {value}"),
      }),
    }
  }
}

impl From<GroupOrder> for u8 {
  fn from(value: GroupOrder) -> Self {
    value as u8
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum TrackStatusCode {
  InProgress = 0x00,
  DoesNotExist = 0x01,
  NotYetBegun = 0x02,
  Finished = 0x03,
  RelayUnavailable = 0x04,
}

impl TryFrom<u64> for TrackStatusCode {
  type Error = ParseError;

  fn try_from(value: u64) -> Result<Self, Self::Error> {
    match value {
      0x00 => Ok(TrackStatusCode::InProgress),
      0x01 => Ok(TrackStatusCode::DoesNotExist),
      0x02 => Ok(TrackStatusCode::NotYetBegun),
      0x03 => Ok(TrackStatusCode::Finished),
      0x04 => Ok(TrackStatusCode::RelayUnavailable),
      _ => Err(ParseError::InvalidType {
        context: "TrackStatusCode::try_from(u64)",
        details: format!("Invalid type, got {value}"),
      }),
    }
  }
}

impl From<TrackStatusCode> for u64 {
  fn from(value: TrackStatusCode) -> Self {
    value as u64
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum SubscribeDoneStatusCode {
  InternalError = 0x0,
  Unauthorized = 0x1,
  TrackEnded = 0x2,
  SubscriptionEnded = 0x3,
  GoingAway = 0x4,
  Expired = 0x5,
  TooFarBehind = 0x6,
}
impl TryFrom<u64> for SubscribeDoneStatusCode {
  type Error = ParseError;

  fn try_from(value: u64) -> Result<Self, Self::Error> {
    match value {
      0x0 => Ok(SubscribeDoneStatusCode::InternalError),
      0x1 => Ok(SubscribeDoneStatusCode::Unauthorized),
      0x2 => Ok(SubscribeDoneStatusCode::TrackEnded),
      0x3 => Ok(SubscribeDoneStatusCode::SubscriptionEnded),
      0x4 => Ok(SubscribeDoneStatusCode::GoingAway),
      0x5 => Ok(SubscribeDoneStatusCode::Expired),
      0x6 => Ok(SubscribeDoneStatusCode::TooFarBehind),
      _ => Err(ParseError::InvalidType {
        context: "SubscribeDoneStatusCode::try_from(u64)",
        details: format!("Invalid type, got {value}"),
      }),
    }
  }
}

impl From<SubscribeDoneStatusCode> for u64 {
  fn from(value: SubscribeDoneStatusCode) -> Self {
    value as u64
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum PublishDoneStatusCode {
  InternalError = 0x0,
  Unauthorized = 0x1,
  TrackEnded = 0x2,
  SubscriptionEnded = 0x3,
  GoingAway = 0x4,
  TooFarBehind = 0x5,
  Expired = 0x6,
  UpdateFailed = 0x8,
  ExcessiveLoad = 0x9,
  MalformedTrack = 0x12,
}

impl TryFrom<u64> for PublishDoneStatusCode {
  type Error = ParseError;

  fn try_from(value: u64) -> Result<Self, Self::Error> {
    match value {
      0x0 => Ok(PublishDoneStatusCode::InternalError),
      0x1 => Ok(PublishDoneStatusCode::Unauthorized),
      0x2 => Ok(PublishDoneStatusCode::TrackEnded),
      0x3 => Ok(PublishDoneStatusCode::SubscriptionEnded),
      0x4 => Ok(PublishDoneStatusCode::GoingAway),
      0x5 => Ok(PublishDoneStatusCode::TooFarBehind),
      0x6 => Ok(PublishDoneStatusCode::Expired),
      0x8 => Ok(PublishDoneStatusCode::UpdateFailed),
      0x9 => Ok(PublishDoneStatusCode::ExcessiveLoad),
      0x12 => Ok(PublishDoneStatusCode::MalformedTrack),
      _ => Err(ParseError::InvalidType {
        context: "PublishDoneStatusCode::try_from(u64)",
        details: format!("Invalid type, got {value}"),
      }),
    }
  }
}

impl PublishDoneStatusCode {
  /// Maps a received status code to a known variant, treating any unknown value
  /// (including GREASE) as `InternalError`. An unknown code is never fatal.
  pub fn from_wire(value: u64) -> Self {
    Self::try_from(value).unwrap_or(Self::InternalError)
  }
}

impl From<PublishDoneStatusCode> for u64 {
  fn from(value: PublishDoneStatusCode) -> Self {
    value as u64
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::model::common::grease::grease_value;

  #[test]
  fn is_first_matches_the_request_opening_types() {
    let first = [
      ControlMessageType::Subscribe,
      ControlMessageType::Fetch,
      ControlMessageType::TrackStatus,
      ControlMessageType::PublishNamespace,
      ControlMessageType::SubscribeNamespace,
      ControlMessageType::SubscribeTracks,
      ControlMessageType::Publish,
    ];
    for t in first {
      assert!(t.is_first(), "{t:?} should open a request stream");
    }
    // A response / control type must not open a request stream.
    for t in [
      ControlMessageType::Setup,
      ControlMessageType::GoAway,
      ControlMessageType::SubscribeOk,
      ControlMessageType::RequestOk,
      ControlMessageType::RequestError,
      ControlMessageType::FetchOk,
    ] {
      assert!(!t.is_first(), "{t:?} must not open a request stream");
    }
  }

  #[test]
  fn publish_done_from_wire_maps_unknown_and_grease_to_internal_error() {
    // Known codes are preserved.
    assert_eq!(
      PublishDoneStatusCode::from_wire(0x2),
      PublishDoneStatusCode::TrackEnded
    );

    // Unknown and grease codes fall back to InternalError instead of failing.
    for raw in [0x7Eu64, grease_value(0).unwrap(), grease_value(5).unwrap()] {
      assert_eq!(
        PublishDoneStatusCode::from_wire(raw),
        PublishDoneStatusCode::InternalError
      );
    }
  }

  #[test]
  fn reserved_publish_ok_v17_is_rejected() {
    assert!(ControlMessageType::try_from(0x1Eu64).is_err());
    // PUBLISH is answered by REQUEST_OK, which still parses.
    assert_eq!(
      ControlMessageType::try_from(0x07u64).unwrap(),
      ControlMessageType::RequestOk
    );
  }

  #[test]
  fn supported_versions_lists_every_version_in_order_of_preference() {
    let listed: Vec<&str> = SUPPORTED_VERSIONS.split(',').collect();
    let all: Vec<&str> = Version::ALL.iter().map(|v| v.alpn()).collect();
    assert_eq!(listed, all);
    assert_eq!(Version::default(), Version::Draft22);
    assert_eq!(listed[0], "moqt-22");
  }

  #[test]
  fn alpn_round_trips_and_refuses_what_it_does_not_speak() {
    for v in Version::ALL {
      assert_eq!(Version::from_alpn(v.alpn().as_bytes()), Some(v));
    }
    for other in ["moqt-18", "moqt-21", "moqt-2", "h3", ""] {
      assert_eq!(Version::from_alpn(other.as_bytes()), None, "{other}");
    }
  }
}
