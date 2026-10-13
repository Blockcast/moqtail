---
'moqtail': patch
---

Keep the bytes a stream reader returns together with `done`: a trailing object delivered with FIN is now parsed, and a partial one is rejected instead of silently dropped
