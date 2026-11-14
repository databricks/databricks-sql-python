# Driver pitfalls

Shared lessons from engineer-bot changes and reviews.

- Preserve public error types, codes, payloads, and documented behavior outside
  the task. A clearer diagnostic must not break callers inspecting the error.
- CloudFetch URLs contain credentials in their query strings. Network error
  messages may include the request URL; use safe status codes or error categories
  rather than copying raw messages into logs or caller-visible errors.
- For logging failures, verify that output still reaches the remaining sinks
  after enough writes to expose buffering stalls. Preserve existing path semantics.
- Filesystem tests must account for platform differences. Synchronous FIFO opens
  can block; permission-denied tests must skip when running as root.
