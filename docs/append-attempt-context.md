# Append request context

The gateway passes the original client attempt ID and requested offset through
`ObjectPlane::append_with_attempt`. The same context is reused for each metadata
retry. This entry point handles an append to an existing object, the first append
to a new key, and an append that revives a tombstone. The attributes supplied for
object creation follow the same path.

`AppendContext::id` stays absent when the client sends no ID. Invalid IDs and
incorrect offsets are refused before invoking the backend. The default trait
implementation uses the existing append and creation methods, preserving the page
backend contract and its error handling.

A native LOG backend can override this entry point to bind the ID and complete
request range to object incarnation and individual durable records. Passing the
context does not by itself prove that an append landed. Durable tag validation,
recovery settlement and default LOG backend integration remain separate work.
