# Simple Stream Serialization Protocol (S3P) v0.1.0

Status: Draft

## Table of Contents

- [Simple Stream Serialization Protocol (S3P) v0.1.0](#simple-stream-serialization-protocol-s3p-v010)
  - [Table of Contents](#table-of-contents)
  - [1. Introduction](#1-introduction)
  - [2. Design Goals](#2-design-goals)
  - [3. Data Types and Framing](#3-data-types-and-framing)
  - [4. General Rules](#4-general-rules)
  - [5. Log Sequence Numbers and Timestamps](#5-log-sequence-numbers-and-timestamps)
  - [6. Stream Naming](#6-stream-naming)
  - [7. Options Encoding](#7-options-encoding)
  - [8. Commands](#8-commands)
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

S3P intentionally trims the RESP surface area to four types: Arrays, Bulk Strings, Simple Strings, and Errors. It keeps RESP’s framing and CRLF line endings, enabling performance comparable to binary protocols while remaining easy to debug.

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
- - After sending any Error reply, servers MUST close the connection immediately. Note: Future versions of this protocol may differentiate between recoverable and unrecoverable errors. In v0.1.0, all errors result in connection closure.
- Clients MAY pipeline multiple commands without waiting for replies. If an Error is produced, the server MUST close the connection and any pipelined replies not yet read by the client are lost.

## 5. Log Sequence Numbers and Timestamps

S3P uses a timestamp format compatible with Redis Streams (`<ms>-<seq>` where `<ms>` and `<seq>` are base-10 unsigned integers without leading sign) to sequence records in a stream. In v0.1.0, traditional sequence numbers like Kafka offsets are not supported, but might be supported in future versions.

- ABNF: `timestamp = 1*DIGIT "-" 1*DIGIT`
- Ordering is lexicographic by `<ms>`, then by `<seq>`.

## 6. Stream Naming

Stream names are Bulk Strings with a minimum length of 1 byte. Names are binary-safe and case-sensitive. Servers SHOULD offer a configurable maximum length limit for stream names and MUST reject names that exceed the configured limit.

## 7. Options Encoding

Options are encoded as an Array of Bulk Strings representing key-value pairs: `[ key1, value1, key2, value2, ... ]`.

- Keys are case-insensitive ASCII. Values are opaque Bulk Strings unless documented otherwise.
- The options array MAY be empty (`*0\r\n`).
- Implementations MUST reject any options array with an odd number of elements.
- Unknown option keys MUST be rejected.
- If a key appears multiple times in the options array, the last occurrence's value MUST be used (last-value-wins semantics).

## 8. Commands

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
3. Array: `options` (key/value pairs, MAY be empty)

Known options:

- `TIMESTAMP_STRATEGY`: Bulk String; values: `server` (default) or `client` (case-insensitive)
  - `server`: the server generates timestamps for APPEND. Clients MUST NOT provide a `TIMESTAMP` option in APPEND requests.
  - `client`: clients generate monotonically increasing timestamps for APPEND (see §8.2). Clients MUST provide a `TIMESTAMP` option in APPEND requests.

Replies:

- `+OK\r\n` on success
- Error on failure (connection then closed):
  - `ERR_BAD_FORMAT`:
    - malformed command or options array
    - name length not in range [1, configured_max]
    - option not recognized
    - invalid option value (e.g., unknown `TIMESTAMP_STRATEGY` value)
  - `ERR_STREAM_EXISTS`: a stream with this name already exists

Example (create with client timestamps):

```
*3
$6
CREATE
$6
orders
*2
$18
TIMESTAMP_STRATEGY
$6
client
```

Reply:

```
+OK
```

Example error (create with unknown option):

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

### 8.2 APPEND

Append one or more records to a stream.

Request schema (Array):

1. Bulk String: `APPEND`
2. Bulk String: `stream-name`
3. Array: `options` (key/value pairs, MAY be empty)
4. Array: `records` (Bulk Strings), MUST contain at least 1 element

Known options:

- `TIMESTAMP`: Bulk String; `<ms>-<seq>` format only. MUST be present if and only if the stream's `TIMESTAMP_STRATEGY` is `client`. MUST NOT be present if the strategy is `server`.

Behavior:

- If the stream's `TIMESTAMP_STRATEGY` is `server`:
  - Clients MUST NOT include a `TIMESTAMP` option.
  - Server assigns monotonically increasing timestamps.
- If `TIMESTAMP_STRATEGY` is `client`:
  - Clients MUST include a `TIMESTAMP` option with an explicit `<ms>-<seq>` timestamp.
  - The provided timestamp MUST be strictly greater than the last appended timestamp for the stream.

Limits and validation:

- `records` MUST have length ≥ 1.
- Each element of `records` MUST be a Bulk String.
- Servers SHOULD enforce the following configurable constraints, rejecting the request if violated:
  - Maximum number of records per append
  - Maximum size (bytes) per record payload
  - Maximum aggregate payload size for the entire append request

Replies:

- `+OK\r\n` on success
- Error on failure (connection then closed):
  - `ERR_UNKNOWN_STREAM`: there is no stream with the provided name
  - `ERR_BAD_FORMAT`:
    - malformed command or options array
    - name length not in range [1, configured_max]
    - option not recognized
    - invalid option value (e.g., bad timestamp format)
    - zero-length records array
    - invalid record bulk string
    - missing timestamp option when required
    - timestamp option present when strategy is set to `server`
    - provided timestamp is lesser than tail (needs to be monotonically increasing)
  - `ERR_LIMITS`:
    - individual record too large (as defined by the server configured limits)
    - aggregate of records in APPEND command too large (as defined by the server configured limits)
    - too many records in APPEND request

Examples:

Server-generated timestamp (`server` strategy):

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
1700000001234-0
```

Client-provided timestamp (`client` strategy):

```
*4
$6
APPEND
$6
orders
*2
$9
TIMESTAMP
$15
1700000001235-0
*1
$7
payload
```

Reply:

```
$15
1700000001235-0
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
- `MIN_TIMESTAMP`: `<ms>-<seq>`; only return records with timestamp strictly greater than this value; defaults to `0-0`

Replies:

- Array: a flat sequence of Bulk Strings in pairs `[ timestamp1, record1, timestamp2, record2, ... ]`; MAY be empty if no records are available within the timeout
- Error on failure (connection then closed):
  - `ERR_UNKNOWN_STREAM`: there is no stream with the provided name
  - `ERR_BAD_FORMAT`:
    - malformed command or options array
    - name length not in range [1, configured_max]
    - option not recognized
    - invalid option value (e.g., bad timestamp format, non-numeric COUNT or BLOCK)
  - `ERR_LIMITS`:
    - COUNT greater than server configured limit
    - BLOCK greater than server configured limit

Semantics:

- If records newer than `MIN_TIMESTAMP` exist, return up to `COUNT` records immediately.
- If none exist and `BLOCK` > 0, wait up to `BLOCK` milliseconds for new records; return an empty array if none arrive.
- Returned records MUST be in strictly increasing timestamp order.

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
$13
MIN_TIMESTAMP
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

Trim (delete) records strictly before a given timestamp.

Request schema (Array):

1. Bulk String: `TRIM`
2. Bulk String: `stream-name`
3. Array: `options` (key/value pairs, requires at least 1)

Known options:

- `UNTIL`: [mandatory] timestamp in the `<ms>-<seq>` format; all records with timestamp strictly less than this value are removed

Replies:

- `+OK\r\n` on success
- Error on failure (connection then closed):
  - `ERR_UNKNOWN_STREAM`: there is no stream with the provided name
  - `ERR_BAD_FORMAT`:
    - malformed command or options array
    - name length not in range [1, configured_max]
    - option not recognized
    - invalid option value (e.g., bad timestamp format)

Example:

```
*3
$4
TRIM
$6
orders
*2
$5
UNTIL
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

Replies:

- `+OK\r\n` on success
- Error on failure (connection then closed):
  - `ERR_UNKNOWN_STREAM`: there is no stream with the provided name
  - `ERR_BAD_FORMAT`:
    - malformed command or options array
    - name length not in range [1, configured_max]
    - option not recognized

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

Errors are Simple Error Strings. The first token up to the first space SHOULD be an uppercase error code for machine parsing, followed by a human-readable message. After an Error is written, the server MUST close the connection.

Format:

```
-<ERROR_CODE> <message>\r\n
```

Canonical error codes defined by this specification:

- `ERR_BAD_FORMAT`: Request is malformed or contains invalid data (e.g., malformed command/options array, invalid option keys/values, invalid timestamp format, constraint violations)
- `ERR_STREAM_EXISTS`: Attempt to create a stream that already exists
- `ERR_UNKNOWN_STREAM`: Operation references a stream that does not exist
- `ERR_LIMITS`: Request exceeds server-configured limits (e.g., record size, record count, COUNT/BLOCK parameter values)

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

When limits are exceeded:

- Servers MUST return `ERR_LIMITS` for COUNT, BLOCK, record size, record count, and aggregate payload violations.
- Servers MUST return `ERR_BAD_FORMAT` for stream name length violations.

## 12. Concurrency and Ordering

- Within a single stream, timestamps MUST be strictly increasing.
- When `TIMESTAMP_STRATEGY=server`, the server MUST generate strictly increasing timestamps even under concurrency.
- When `TIMESTAMP_STRATEGY=client`, the server MUST reject APPENDs whose timestamps are ≤ the last appended timestamp.
- READ responses MUST be ordered by timestamp strictly increasing.

## 13. Pipelining and Connection Handling

- Clients MAY pipeline commands. Servers MUST reply in order.
- Upon any Error reply, the server MUST close the connection immediately. Pipelined but unread replies are discarded by the close.

## 14. Compatibility and Extensibility

- This document defines S3P v0.1.0.
- Unknown options MUST be rejected, ensuring strict schemas for interoperability.
- New commands or options MAY be added in future versions. Clients SHOULD treat unknown commands/options as errors.

## 15. Security Considerations

- Implementations MUST bound memory usage by enforcing configured limits.
- Inputs MUST be validated thoroughly to avoid request smuggling or buffer overflows (e.g., malformed lengths, negative numbers, non-decimal values where numbers are expected).
- Connections MUST be closed upon error to avoid ambiguous state.

## 16. Data Integrity

S3P is a simple text-based protocol that relies on the underlying transport layer (TCP) to handle data integrity during transmission.

TCP includes a 16-bit checksum in every segment that detects corruption. If corruption is detected, TCP automatically retransmits the corrupted segment. TCP also ensures data arrives in the correct order. In practice, bit flips during network transmission are caught and corrected by TCP before S3P ever sees them.

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

 timestamp        = 1*DIGIT "-" 1*DIGIT

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
 delete-cmd       = array-of-2
```

---

End of S3P v0.1.0 Specification.
