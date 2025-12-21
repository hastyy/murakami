package protocol

import (
	"time"
)

type CreateCommand struct {
	StreamName string
}

type AppendCommand struct {
	StreamName string
	Records    [][]byte
	Options    AppendCommandOptions
}

type AppendCommandOptions struct {
	MillisID string
}

type ReadCommand struct {
	StreamName string
	Options    ReadCommandOptions
}

type ReadCommandOptions struct {
	Count int
	Block time.Duration
	MinID string
}

type TrimCommand struct {
	StreamName string
	Options    TrimCommandOptions
}

type TrimCommandOptions struct {
	MinID string
}

type DeleteCommand struct {
	StreamName string
}
