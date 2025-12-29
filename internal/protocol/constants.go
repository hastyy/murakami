package protocol

const (
	// Known symbols
	symbolArray        = '*'
	symbolBulkString   = '$'
	symbolSimpleString = '+'
	symbolError        = '-'

	// CRLF
	separatorCRLF = "\r\n"

	// Command names
	CommandCreate = "CREATE"
	CommandAppend = "APPEND"
	CommandRead   = "READ"
	CommandTrim   = "TRIM"
	CommandDelete = "DELETE"

	// Option names
	optionID    = "ID"
	optionMinID = "MIN_ID"
	optionCount = "COUNT"
	optionBlock = "BLOCK"

	// Reply strings
	ReplyOK       = "+OK\r\n"
	replyOKString = "OK"
)
