---
'moqtail': patch
---

Stop the control-message loop from rethrowing when the session ends, which surfaced every peer close of the control stream as an unhandled promise rejection
