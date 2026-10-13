---
'moqtail': minor
'moqtail-rs': minor
'relay': minor
---

Negotiate draft-20 by ALPN beside draft-22. Draft-22 stays the default and is preferred whenever both ends offer it; draft-20 sessions write LOCATION_FILTER in the draft-20 length-prefixed form. The TS client reads the protocol the server selected, the Rust `ControlStreamHandler` takes the session's `Version`, and the relay records each session's negotiated version.
