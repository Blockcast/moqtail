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

use crate::model::common::varint::BufVarIntExt;
use crate::model::error::ParseError;
use bytes::{Buf, Bytes};

use super::{
  constant::{ControlMessageType, Version},
  fetch::Fetch,
  fetch_ok::FetchOk,
  goaway::GoAway,
  namespace::Namespace,
  namespace_done::NamespaceDone,
  publish::Publish,
  publish_done::PublishDone,
  publish_namespace::PublishNamespace,
  publish_skipped::PublishSkipped,
  publish_state_notify::PublishStateNotify,
  request_error::RequestError,
  request_ok::RequestOk,
  request_update::RequestUpdate,
  setup::Setup,
  subscribe::Subscribe,
  subscribe_namespace::SubscribeNamespace,
  subscribe_ok::SubscribeOk,
  subscribe_tracks::SubscribeTracks,
  track_status::TrackStatus,
};

#[derive(Debug, Clone, PartialEq)]
pub enum ControlMessage {
  Namespace(Box<Namespace>),
  NamespaceDone(Box<NamespaceDone>),
  PublishNamespace(Box<PublishNamespace>),
  RequestOk(Box<RequestOk>),
  Publish(Box<Publish>),
  PublishDone(Box<PublishDone>),
  Fetch(Box<Fetch>),
  FetchOk(Box<FetchOk>),
  Goaway(Box<GoAway>),
  Setup(Box<Setup>),
  Subscribe(Box<Subscribe>),
  SubscribeOk(Box<SubscribeOk>),
  RequestUpdate(Box<RequestUpdate>),
  TrackStatus(Box<TrackStatus>),
  SubscribeNamespace(Box<SubscribeNamespace>),
  SubscribeTracks(Box<SubscribeTracks>),
  RequestError(Box<RequestError>),
  PublishSkipped(Box<PublishSkipped>),
  PublishStateNotify(Box<PublishStateNotify>),
}

pub trait ControlMessageTrait: std::fmt::Debug {
  fn serialize(&self) -> Result<Bytes, ParseError>;
  fn parse_payload(payload: &mut Bytes) -> Result<Box<Self>, ParseError>
  where
    Self: Sized;
  fn get_type(&self) -> ControlMessageType;

  /// Serializes for the wire of a negotiated `version`. Only a message that carries
  /// parameters can differ between versions, and only that kind overrides this.
  fn serialize_for(&self, _version: Version) -> Result<Bytes, ParseError> {
    self.serialize()
  }

  /// Parses a payload read on the wire of a negotiated `version`.
  fn parse_payload_for(payload: &mut Bytes, _version: Version) -> Result<Box<Self>, ParseError>
  where
    Self: Sized,
  {
    Self::parse_payload(payload)
  }
}

impl ControlMessage {
  pub fn deserialize(bytes: &mut Bytes) -> Result<Self, ParseError> {
    Self::deserialize_for(bytes, Version::default())
  }

  /// [`Self::deserialize`] for the wire of a negotiated `version`.
  pub fn deserialize_for(bytes: &mut Bytes, version: Version) -> Result<Self, ParseError> {
    let message_type = bytes.get_vi()?;
    let msg_type = ControlMessageType::try_from(message_type)?;

    if bytes.remaining() < 2 {
      return Err(ParseError::NotEnoughBytes {
        context: "ControlMessage::deserialize(payload_length)",
        needed: 2,
        available: 0,
      });
    }
    let payload_length = bytes.get_u16() as usize;

    if bytes.remaining() < payload_length {
      return Err(ParseError::NotEnoughBytes {
        context: "ControlMessage::deserialize(payload_length)",
        needed: payload_length,
        available: bytes.remaining(),
      });
    }

    let mut payload = bytes.copy_to_bytes(payload_length);
    let message = match msg_type {
      ControlMessageType::Namespace => {
        Namespace::parse_payload_for(&mut payload, version).map(ControlMessage::Namespace)
      }
      ControlMessageType::NamespaceDone => {
        NamespaceDone::parse_payload_for(&mut payload, version).map(ControlMessage::NamespaceDone)
      }
      ControlMessageType::PublishNamespace => {
        PublishNamespace::parse_payload_for(&mut payload, version)
          .map(ControlMessage::PublishNamespace)
      }
      ControlMessageType::RequestError => {
        RequestError::parse_payload_for(&mut payload, version).map(ControlMessage::RequestError)
      }
      ControlMessageType::RequestOk => {
        RequestOk::parse_payload_for(&mut payload, version).map(ControlMessage::RequestOk)
      }
      ControlMessageType::Publish => {
        Publish::parse_payload_for(&mut payload, version).map(ControlMessage::Publish)
      }
      ControlMessageType::PublishDone => {
        PublishDone::parse_payload_for(&mut payload, version).map(ControlMessage::PublishDone)
      }
      ControlMessageType::Setup => {
        Setup::parse_payload_for(&mut payload, version).map(ControlMessage::Setup)
      }
      ControlMessageType::Fetch => {
        Fetch::parse_payload_for(&mut payload, version).map(ControlMessage::Fetch)
      }
      ControlMessageType::FetchOk => {
        FetchOk::parse_payload_for(&mut payload, version).map(ControlMessage::FetchOk)
      }
      ControlMessageType::GoAway => {
        GoAway::parse_payload_for(&mut payload, version).map(ControlMessage::Goaway)
      }
      ControlMessageType::Subscribe => {
        Subscribe::parse_payload_for(&mut payload, version).map(ControlMessage::Subscribe)
      }
      ControlMessageType::SubscribeOk => {
        SubscribeOk::parse_payload_for(&mut payload, version).map(ControlMessage::SubscribeOk)
      }
      ControlMessageType::RequestUpdate => {
        RequestUpdate::parse_payload_for(&mut payload, version).map(ControlMessage::RequestUpdate)
      }
      ControlMessageType::TrackStatus => {
        TrackStatus::parse_payload_for(&mut payload, version).map(ControlMessage::TrackStatus)
      }
      ControlMessageType::SubscribeNamespace => {
        SubscribeNamespace::parse_payload_for(&mut payload, version)
          .map(ControlMessage::SubscribeNamespace)
      }
      ControlMessageType::SubscribeTracks => {
        SubscribeTracks::parse_payload_for(&mut payload, version)
          .map(ControlMessage::SubscribeTracks)
      }
      ControlMessageType::PublishSkipped => {
        PublishSkipped::parse_payload_for(&mut payload, version).map(ControlMessage::PublishSkipped)
      }
      ControlMessageType::PublishStateNotify => {
        PublishStateNotify::parse_payload_for(&mut payload, version)
          .map(ControlMessage::PublishStateNotify)
      }
    }
    .map_err(|err| ParseError::ProtocolViolation {
      context: "ControlMessage::deserialize(payload)",
      details: err.to_string(),
    })?;

    if payload.has_remaining() {
      return Err(ParseError::ProtocolViolation {
        context: "ControlMessage::deserialize(final_check)",
        details: format!(
          "Extra {} bytes remaining in payload after parsing",
          payload.remaining()
        ),
      });
    };
    Ok(message)
  }

  pub fn serialize(&self) -> Result<Bytes, ParseError> {
    self.serialize_for(Version::default())
  }

  /// [`Self::serialize`] for the wire of a negotiated `version`.
  pub fn serialize_for(&self, version: Version) -> Result<Bytes, ParseError> {
    match self {
      ControlMessage::Namespace(msg) => msg.serialize_for(version),
      ControlMessage::NamespaceDone(msg) => msg.serialize_for(version),
      ControlMessage::PublishNamespace(msg) => msg.serialize_for(version),
      ControlMessage::RequestError(msg) => msg.serialize_for(version),
      ControlMessage::RequestOk(msg) => msg.serialize_for(version),
      ControlMessage::Publish(msg) => msg.serialize_for(version),
      ControlMessage::PublishDone(msg) => msg.serialize_for(version),
      ControlMessage::Fetch(msg) => msg.serialize_for(version),
      ControlMessage::FetchOk(msg) => msg.serialize_for(version),
      ControlMessage::Goaway(msg) => msg.serialize_for(version),
      ControlMessage::Setup(msg) => msg.serialize_for(version),
      ControlMessage::Subscribe(msg) => msg.serialize_for(version),
      ControlMessage::SubscribeOk(msg) => msg.serialize_for(version),
      ControlMessage::RequestUpdate(msg) => msg.serialize_for(version),
      ControlMessage::TrackStatus(msg) => msg.serialize_for(version),
      ControlMessage::SubscribeNamespace(msg) => msg.serialize_for(version),
      ControlMessage::SubscribeTracks(msg) => msg.serialize_for(version),
      ControlMessage::PublishSkipped(msg) => msg.serialize_for(version),
      ControlMessage::PublishStateNotify(msg) => msg.serialize_for(version),
    }
  }

  /// Returns the message type of the control message.
  pub fn get_type(&self) -> ControlMessageType {
    match self {
      ControlMessage::Namespace(_) => ControlMessageType::Namespace,
      ControlMessage::NamespaceDone(_) => ControlMessageType::NamespaceDone,
      ControlMessage::PublishNamespace(_) => ControlMessageType::PublishNamespace,
      ControlMessage::RequestError(_) => ControlMessageType::RequestError,
      ControlMessage::RequestOk(_) => ControlMessageType::RequestOk,
      ControlMessage::Publish(_) => ControlMessageType::Publish,
      ControlMessage::PublishDone(_) => ControlMessageType::PublishDone,
      ControlMessage::Fetch(_) => ControlMessageType::Fetch,
      ControlMessage::FetchOk(_) => ControlMessageType::FetchOk,
      ControlMessage::Goaway(_) => ControlMessageType::GoAway,
      ControlMessage::Setup(_) => ControlMessageType::Setup,
      ControlMessage::Subscribe(_) => ControlMessageType::Subscribe,
      ControlMessage::SubscribeOk(_) => ControlMessageType::SubscribeOk,
      ControlMessage::RequestUpdate(_) => ControlMessageType::RequestUpdate,
      ControlMessage::TrackStatus(_) => ControlMessageType::TrackStatus,
      ControlMessage::SubscribeNamespace(_) => ControlMessageType::SubscribeNamespace,
      ControlMessage::SubscribeTracks(_) => ControlMessageType::SubscribeTracks,
      ControlMessage::PublishSkipped(_) => ControlMessageType::PublishSkipped,
      ControlMessage::PublishStateNotify(_) => ControlMessageType::PublishStateNotify,
    }
  }

  /// The Request ID a request message consumes, or `None` for a message that is not
  /// a request. Responses carry none: they travel on their request's own stream.
  pub fn request_id(&self) -> Option<u64> {
    match self {
      ControlMessage::Subscribe(m) => Some(m.request_id),
      ControlMessage::Publish(m) => Some(m.request_id),
      ControlMessage::Fetch(m) => Some(m.request_id),
      ControlMessage::SubscribeNamespace(m) => Some(m.request_id),
      ControlMessage::SubscribeTracks(m) => Some(m.request_id),
      ControlMessage::PublishNamespace(m) => Some(m.request_id),
      ControlMessage::RequestUpdate(m) => Some(m.request_id),
      ControlMessage::TrackStatus(m) => Some(m.request_id),
      _ => None,
    }
  }
}

#[cfg(test)]
mod tests {
  use crate::model::{
    common::tuple::Tuple,
    parameter::{authorization_token::AuthorizationToken, message_parameter::MessageParameter},
  };

  use super::*;

  #[test]
  fn a_request_reports_its_request_id() {
    let update = ControlMessage::RequestUpdate(Box::new(RequestUpdate::new(42, vec![])));
    assert_eq!(update.request_id(), Some(42));
  }

  #[test]
  fn a_message_that_is_not_a_request_has_no_request_id() {
    let goaway = ControlMessage::Goaway(Box::new(GoAway::new(None, 0)));
    assert_eq!(goaway.request_id(), None);
  }

  #[test]
  fn test_announce_roundtrip() {
    let request_id = 12345;
    let track_namespace = Tuple::from_utf8_path("god/dayyum");
    let parameters = vec![MessageParameter::new_authorization_token(
      AuthorizationToken::new_use_value(0, Bytes::from_static(b"test-token")),
    )];
    let announce = PublishNamespace {
      request_id,
      track_namespace,
      parameters,
    };

    let mut buf = announce.serialize().unwrap();
    let deserialized = ControlMessage::deserialize(&mut buf).unwrap();
    if let ControlMessage::PublishNamespace(deserialized_announce) = deserialized {
      assert_eq!(*deserialized_announce, announce);
    } else {
      panic!("Expected ControlMessage::PublishNamespace variant");
    }
    assert!(!buf.has_remaining());
  }
}
