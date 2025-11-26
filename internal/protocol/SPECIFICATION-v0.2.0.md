# Simple Stream Serialization Protocol (S3P) v0.2.0

Status: Draft

## Table of Contents

- [Simple Stream Serialization Protocol (S3P) v0.2.0](#simple-stream-serialization-protocol-s3p-v020)
  - [Table of Contents](#table-of-contents)
  - [1. Introduction](#1-introduction)
  - [2. Design Goals](#2-design-goals)
  - [3. Data Types and Framing](#3-data-types-and-framing)
  - [4. General Rules](#4-general-rules)
  - [5. IDs, Timestamps and Sequence Numbers](#5-ids-timestamps-and-sequence-numbers)
  - [6. Stream Naming](#6-stream-naming)
  - [7. Options Encoding](#7-options-encoding)
    - [8.1 CREATE](#81-create)
    - [8.2 APPEND](#82-append)
    - [8.3 READ](#83-read)
    - [8.4 TRIM](#84-trim)
    - [8.5 DELETE](#85-delete)
  - [9. Error Format](#9-error-format)
  - [10. Case Insensitivity](#10-case-insensitivity)
  - [11. Configuration and Limits](#11-configuration-and-limits)
  - [12. Concurrency and Ordering](#12-concurrency-and-ordering)
  - [13. Pipelining and Connection Handling](#13-pipelining-and-connection-handling)
  - [14. Compatibility and Extensibility](#14-compatibility-and-extensibility)
  - [15. Security Considerations](#15-security-considerations)
  - [16. Data Integrity](#16-data-integrity)
  - [17. Appendix A: Non-normative ABNF (excerpt)](#17-appendix-a-non-normative-abnf-excerpt)

## 1. Introduction

Simple Stream Serialization Protocol (S3P) is a text-framed, binary-safe wire protocol inspired by the Redis Serialization Protocol (RESP). S3P is designed to be simple to implement, fast to parse, human-readable, and focused on append-only stream use cases similar to Redis Streams.

S3P intentionally trims the RESP surface area to four types: Arrays, Bulk Strings, Simple Strings, and Errors. It keeps RESP's framing and CRLF line endings, enabling performance comparable to binary protocols while remaining easy to debug.

This document uses the key words MUST, MUST NOT, REQUIRED, SHALL, SHALL NOT, SHOULD, SHOULD NOT, RECOMMENDED, NOT RECOMMENDED, MAY, and OPTIONAL as described in RFC 2119.

## 2. Design Goals

- Simplicity and performance comparable to RESP
- Human-readable and easy to debug
- Binary-safe bulk data transfer
- Case-insensitive commands and option keys
- Deterministic, streaming-friendly framing with CRLF

## 3. Data Types and Framing

S3P reuses RESP framing and CRLF ("\r\n") line endings exactly.

- Simple String: `+<ascii-text>\r\n`
- Error: `-<ascii-text>\r\n`
- Bulk String: `$<length>\r\n<binary-bytes>\r\n`
  - `<length>` is the number of raw bytes in `<binary-bytes>`
  - Zero-length bulk strings are not allowed anywhere in S3P. `$<length>` MUST be ≥ 1. Servers MUST reject `$0` (and any zero-length bulk string) with `ERR_BAD_FORMAT`.
- Array: `*<num-elements>\r\n<element-1>...<element-N>`
  - Arrays MAY be empty: `*0\r\n`
  - Array elements MAY be any of the supported types (Simple String, Error, Bulk String, or Array)
  - Within a single array, all elements MUST be of the same type (arrays MUST NOT contain mixed types)

Unsupported RESP types (e.g., integers, nulls) are excluded from S3P. Where "no value" is required, use empty arrays as specified by this document.

Servers and clients MUST use CRLF as the only line separator. Implementations MUST NOT accept or emit LF-only or CR-only separators.

## 4. General Rules

- Commands from client to server MUST be sent as a top-level Array. Each element MUST be either a Bulk String or another Array, according to the command's schema.
- Server replies MAY be any supported top-level type (Simple String, Error, Bulk String, Array), as defined per command.
- Commands and option keys are case-insensitive ASCII. Servers MUST match command names and option keys case-insensitively. Values (including stream names and record payloads) are treated as opaque binary and MUST be preserved exactly.
- S3P differentiates between recoverable and unrecoverable errors:
  - **Unrecoverable errors** (protocol-level): Servers MUST close the connection immediately after sending these errors. These include malformed commands, invalid framing, syntax errors, bad structure, or any situation where the server cannot determine which bytes to expect next in the stream.
  - **Recoverable errors** (application-level): Servers MUST keep the connection open after sending these errors. These are expected error conditions that occur during normal operation, such as attempting to create a stream that already exists or referencing a non-existent stream. The command was successfully parsed and processed, but an error condition was encountered.
- Clients MAY pipeline multiple commands without waiting for replies. If an unrecoverable error is produced, the server MUST close the connection and any pipelined replies not yet read by the client are lost. If a recoverable error is produced, the connection remains open and the client can continue sending commands.

## 5. IDs, Timestamps and Sequence Numbers

S3P uses an ID format compatible with Redis Streams (`<ms>-<seq>` where `<ms>` and `<seq>` are base-10 unsigned integers without leading sign) to sequence records in a stream.

For server-generated IDs:

- `<ms>` is the number of milliseconds since the Unix epoch (January 1, 1970, 00:00:00 UTC)
- `<seq>` is a sequence number within that millisecond timestamp, used to resolve collisions when multiple records are appended within the same millisecond

ID bounds:

- The minimum ID is `0-0`
- The theoretical maximum ID is `18446744073709551615-18446744073709551615` (the maximum value for 64-bit unsigned integers)

When clients specify IDs for their appends, they MUST only specify the timestamp part (`<ms>`). The server will automatically assign the sequence number (`<seq>`) to ensure monotonic ordering. Clients MUST guarantee that the timestamp IDs they provide are monotonically increasing.

Note: Monotonically increasing: A sequence where each value is greater than or equal to the previous one.

## 6. Stream Naming

Stream names are Bulk Strings with a minimum length of 1 byte. Names are binary-safe and case-sensitive. Servers SHOULD offer a configurable maximum length limit for stream names and MUST reject names that exceed the configured limit.

## 7. Options Encoding

Options are encoded as an Array of Bulk Strings representing key-value pairs: `[ key1, value1, key2, value2, ... ]`.

- Keys are case-insensitive ASCII. Values are opaque Bulk Strings unless documented otherwise.
- The options array MAY be empty (`*0\r\n`).
- Implementations MUST reject any options array with an odd number of elements.
- Unknown option keys MUST be rejected.
- If a key appears multiple times in the options array, the last occurrence's value MUST be used (last-value-wins semantics).

All commands are sent as a top-level Array. The first element MUST be the command name as a Bulk String. This specification defines the following commands (names are case-insensitive):

- `CREATE`
- `APPEND`
- `READ`
- `TRIM`
- `DELETE`

### 8.1 CREATE

Create a new stream.

Request schema (Array):

1. Bulk String: `CREATE`
2. Bulk String: `stream-name`
3. Array: `options` (key/value pairs, empty for now)

Note: The options array is included in the command schema for forward compatibility and ease of expansion, even though no options are currently defined for this command.

Replies:

- `+OK\r\n` on success
- Error on failure:
  - `ERR_BAD_FORMAT` (unrecoverable - connection then closed):
    - malformed command or options array
    - option not recognized
  - `ERR_LIMITS` (unrecoverable - connection then closed):
    - stream name length not in range [1, configured_max]
  - `ERR_STREAM_EXISTS` (recoverable - connection stays open): a stream with this name already exists

Examples:

Example 1 (success):

```
*3
$6
CREATE
$6
orders
*0
```

Reply:

```
+OK
```

Example 2 (bad format error - connection closed):

```
*3
$6
CREATE
$6
orders
*2
$8
MAX_SIZE
$4
1000
```

Reply:

```
-ERR_BAD_FORMAT unknown option MAX_SIZE
```

Example 3 (stream exists error - connection stays open):

```
*3
$6
CREATE
$6
orders
*0
```

Reply:

```
-ERR_STREAM_EXISTS stream orders already exists
```

### 8.2 APPEND

Append one or more records to a stream.

Request schema (Array):

1. Bulk String: `APPEND`
2. Bulk String: `stream-name`
3. Array: `options` (key/value pairs, MAY be empty)
4. Array: `records` (Bulk Strings), MUST contain at least 1 element

Known options:

- `ID`: Bulk String; specifies only the timestamp part (`<ms>`) as an unsigned integer within the uint64 range. The server automatically assigns the sequence number (`<seq>`) to ensure monotonic ordering. Clients are responsible for ensuring that the timestamp values they provide are monotonically increasing.

Limits and validation:

- `records` array MUST have length ≥ 1.
- Each element of `records` MUST be a Bulk String.
- Servers SHOULD enforce the following configurable constraints, rejecting the request if violated:
  - Maximum number of records per append
  - Maximum size (bytes) per record payload
  - Maximum aggregate payload size for the entire append request

Replies:

- Bulk string with last appended ID (`<ms>-<seq>`) on success
- Error on failure:
  - `ERR_BAD_FORMAT` (unrecoverable - connection then closed):
    - malformed command or options array
    - option not recognized
    - invalid option value (e.g., bad ID format - not an unsigned integer in the uint64 range)
    - zero-length records array
    - invalid record bulk string (length < 1)
  - `ERR_LIMITS` (unrecoverable - connection then closed):
    - stream name length not in range [1, configured_max]
    - individual record too large (as defined by the server configured limits)
    - aggregate of records in APPEND command too large (as defined by the server configured limits)
    - too many records in APPEND request
  - `ERR_NON_MONOTONIC_ID` (recoverable - connection stays open): the provided timestamp ID is not greater than or equal to the last appended ID in the stream
  - `ERR_UNKNOWN_STREAM` (recoverable - connection stays open): there is no stream with the provided name

Examples:

Example 1 (success with two records):

```
*4
$6
APPEND
$6
orders
*0
*2
$5
hello
$5
world
```

Reply:

```
$15
1700000001234-1
```

Example 2 (bad format error - zero-length record):

```
*4
$6
APPEND
$6
orders
*0
*2
$5
hello
$0

```

Reply:

```
-ERR_BAD_FORMAT invalid record bulk string (length < 1)
```

Example 3 (non-monotonic ID error - connection stays open):

```
*4
$6
APPEND
$6
orders
*2
$2
ID
$13
1700000001000
*1
$7
payload
```

Reply (assuming last appended ID was 1700000001234-1):

```
-ERR_NON_MONOTONIC_ID provided timestamp ID 1700000001000 is not greater than last appended ID 1700000001234
```

### 8.3 READ

Read records from a stream, optionally blocking.

Request schema (Array):

1. Bulk String: `READ`
2. Bulk String: `stream-name`
3. Array: `options` (key/value pairs, MAY be empty)

Known options:

- `COUNT`: number (ASCII decimal in Bulk String), inclusive range [1, max-configured]; defaults to a server-configured value; is the max number of records that can be returned per request
- `BLOCK`: milliseconds to wait for new entries if none available (ASCII decimal in Bulk String); defaults to `0`; MUST be ≤ max-configured
- `MIN_ID`: `<ms>-<seq>`; only return records with ID greater or equal to this value; defaults to `0-0`

Replies:

- Array: a flat sequence of Bulk Strings in pairs `[ id1, record1, id2, record2, ... ]`; MAY be empty if no records are available within the timeout
- Error on failure:
  - `ERR_BAD_FORMAT` (unrecoverable - connection then closed):
    - malformed command or options array
    - option not recognized
    - invalid option value (e.g., bad ID format - not an unsigned integer in the uint64 range)
  - `ERR_LIMITS` (unrecoverable - connection then closed):
    - stream name length not in range [1, configured_max]
    - COUNT greater than server configured limit or lower than 1
    - BLOCK greater than server configured limit or lower than 0
  - `ERR_UNKNOWN_STREAM` (recoverable - connection stays open): there is no stream with the provided name

Semantics:

- If records newer than `MIN_ID` exist, return up to `COUNT` records immediately.
- If none exist and `BLOCK` > 0, wait up to `BLOCK` milliseconds for new records; return an empty array if none arrive.
- Returned records MUST be in strictly increasing ID order.

Example (non-blocking read):

```
*3
$4
READ
$6
orders
*4
$5
COUNT
$2
10
$6
MIN_ID
$3
0-0
```

Reply (two records):

```
*4
$15
1700000001234-0
$5
hello
$15
1700000001235-0
$5
world
```

### 8.4 TRIM

Trim (delete) records strictly before a given ID.

Request schema (Array):

1. Bulk String: `TRIM`
2. Bulk String: `stream-name`
3. Array: `options` (key/value pairs, requires at least 1)

Known options:

- `MIN_ID`: [mandatory] ID in the `<ms>-<seq>` format; all records with ID strictly less than this value are removed

Replies:

- `+OK\r\n` on success
- Error on failure:
  - `ERR_BAD_FORMAT` (unrecoverable - connection then closed):
    - malformed command or options array
    - option not recognized
    - invalid option value (e.g., bad ID format)
  - `ERR_LIMITS` (unrecoverable - connection then closed):
    - stream name length not in range [1, configured_max]
  - `ERR_UNKNOWN_STREAM` (recoverable - connection stays open): there is no stream with the provided name

Example:

```
*3
$4
TRIM
$6
orders
*2
$6
MIN_ID
$15
1700000001235-0
```

Reply:

```
+OK
```

### 8.5 DELETE

Delete a stream and all its records.

Request schema (Array):

1. Bulk String: `DELETE`
2. Bulk String: `stream-name`
3. Array: `options` (key/value pairs, empty for now)

Note: The options array is included in the command schema for forward compatibility and ease of expansion, even though no options are currently defined for this command.

Replies:

- `+OK\r\n` on success
- Error on failure:
  - `ERR_BAD_FORMAT` (unrecoverable - connection then closed):
    - malformed command or options array
    - option not recognized
  - `ERR_LIMITS` (unrecoverable - connection then closed):
    - stream name length not in range [1, configured_max]
  - `ERR_UNKNOWN_STREAM` (recoverable - connection stays open): there is no stream with the provided name

Example:

```
*3
$6
DELETE
$6
orders
*0
```

Reply:

```
+OK
```

## 9. Error Format

Errors are Simple Error Strings. The first token up to the first space SHOULD be an uppercase error code for machine parsing, followed by a human-readable message.

Format:

```
-<ERROR_CODE> <message>\r\n
```

S3P v0.2.0 differentiates between recoverable and unrecoverable errors:

- **Unrecoverable errors**: After sending an unrecoverable error, the server MUST close the connection immediately.
- **Recoverable errors**: After sending a recoverable error, the server MUST keep the connection open and the client can continue sending commands.

Canonical error codes defined by this specification:

**Unrecoverable errors** (connection closes after error):

- `ERR_BAD_FORMAT`: Request is malformed or contains invalid data (e.g., malformed command/options array, invalid option keys/values, invalid ID format, constraint violations, zero-length bulk strings where not allowed)
- `ERR_LIMITS`: Request exceeds server-configured limits (e.g., record size, record count, COUNT/BLOCK parameter values)

**Recoverable errors** (connection stays open after error):

- `ERR_STREAM_EXISTS`: Attempt to create a stream that already exists
- `ERR_UNKNOWN_STREAM`: Operation references a stream that does not exist
- `ERR_NON_MONOTONIC_ID`: Provided ID is not monotonically increasing relative to the last appended ID in the stream

## 10. Case Insensitivity

- Command names and option keys MUST be matched case-insensitively (ASCII only).
- Option values, stream names, and record payloads are opaque and case-sensitive by default.

## 11. Configuration and Limits

Servers SHOULD provide configuration for the following limits:

- **READ limits:**
  - Default `COUNT` (used when COUNT option is omitted)
  - Maximum `COUNT`
  - Maximum `BLOCK` duration (milliseconds)
- **APPEND limits:**
  - Maximum number of records per APPEND request
  - Maximum size per individual record (bytes)
  - Maximum aggregate payload size per APPEND request
- **Stream naming:**
  - Maximum stream name length (minimum is always 1 byte per §6)
- **Connection limits (RECOMMENDED):**
  - Idle connection timeout
  - Maximum concurrent connections

When limits are exceeded, servers MUST return `ERR_LIMITS` for COUNT, BLOCK, record size, record count, aggregate payload violations, and stream name length violations.

## 12. Concurrency and Ordering

Within a single stream, IDs MUST be strictly increasing. This section describes how servers and clients ensure this guarantee.

**Server-generated IDs:**

- The server generates the `<ms>` part from its local system clock (milliseconds since Unix epoch)
- The server uses the `<seq>` part to handle collisions when multiple records are appended within the same millisecond
- When a new record is appended:
  - If the current clock time is greater than the last appended `<ms>`, the server uses the new clock time and sets `<seq>` to 0
  - If the current clock time equals the last appended `<ms>`, the server uses the same `<ms>` and increments `<seq>` by 1
  - If the current clock time is lower than the last appended `<ms>` (e.g., due to clock adjustment), the server uses the last appended `<ms>` and increments `<seq>` by 1
  - The server MUST ensure strict monotonicity even under high concurrency

**Client-specified IDs:**

- When clients provide an ID via the `ID` option in APPEND, they MUST only specify the timestamp part (`<ms>`)
- Clients are responsible for ensuring that the timestamp values they provide are monotonically increasing (each timestamp must be greater than or equal to the previous one)
- The server still controls the `<seq>` part and assigns it automatically to ensure strict monotonicity, even when the client provides the timestamp
- The server MUST reject APPENDs where the client-provided timestamp is not greater than or equal to the `<ms>` part of the last appended ID

**Ordering guarantees:**

- Within a single stream, IDs MUST be strictly increasing across all APPENDs
- READ responses MUST return records ordered by ID in strictly increasing order
- Servers MUST maintain these ordering guarantees even under concurrent operations from multiple clients

## 13. Pipelining and Connection Handling

- Clients MAY pipeline commands. Servers MUST reply in order.
- Upon any unrecoverable error reply, the server MUST close the connection immediately. Pipelined but unread replies are discarded by the close.
- Upon a recoverable error reply, the server MUST keep the connection open. The client can continue sending commands, and any pipelined commands will continue to be processed.

## 14. Compatibility and Extensibility

- This document defines S3P v0.2.0.
- Unknown options MUST be rejected, ensuring strict schemas for interoperability.
- New commands or options MAY be added in future versions. Clients SHOULD treat unknown commands/options as errors.

## 15. Security Considerations

- Implementations MUST bound memory usage by enforcing configured limits.
- Inputs MUST be validated thoroughly to avoid request smuggling or buffer overflows (e.g., malformed lengths, negative numbers, non-decimal values where numbers are expected).
- Connections MUST be closed upon unrecoverable errors to avoid ambiguous protocol state.

## 16. Data Integrity

S3P is a simple text-based protocol that relies on the underlying transport layer (TCP) to handle data integrity during transmission.

TCP includes a 16-bit checksum in every segment that detects corruption. If corruption is detected, TCP automatically retransmits the corrupted segment. TCP also ensures data is delivered in the correct order. In practice, bit flips during network transmission are caught and corrected by TCP before S3P ever sees them.

S3P's philosophy is to rely on TCP for transport-level integrity and let applications implement end-to-end integrity checks if needed (e.g., application-level checksums in record payloads). This keeps the protocol simple and fast.

## 17. Appendix A: Non-normative ABNF (excerpt)

The following ABNF excerpts are informational. Full RESP framing is assumed per §3.

```
 ALPHA   = %x41-5A / %x61-7A
 DIGIT   = %x30-39
 CRLF    = %x0D %x0A

 s3p-command      = array
 array            = "*" 1*DIGIT CRLF *(type)
 bulk-string      = "$" 1*DIGIT CRLF 1*OCTET CRLF ; length >= 1
 simple-string    = "+" *(%x20-7E) CRLF
 error            = "-" *(%x20-7E) CRLF

 id               = 1*DIGIT "-" 1*DIGIT

 ; CREATE
 create-cmd       = array-of-3
 array-of-3       = "*3" CRLF bulk-string bulk-string options-array
 options-array    = array ; even number of bulk-string elements

 ; APPEND
 append-cmd       = array-of-4
 array-of-4       = "*4" CRLF bulk-string bulk-string options-array records-array
 records-array    = array ; >= 1 elements, each a bulk-string

 ; READ
 read-cmd         = array-of-3

 ; TRIM
 trim-cmd         = array-of-3

 ; DELETE
 delete-cmd       = array-of-3
```

---

End of S3P v0.2.0 Specification.
