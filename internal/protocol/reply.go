package protocol

type CreateReply struct {
	Ok  bool
	Err Error
}

type AppendReply struct {
	ID  string
	Err Error
}

type ReadReply struct {
	RecordSequence []SequencePair
	Err            Error
}

type SequencePair struct {
	ID     string
	Record []byte
}

type TrimReply struct {
	Ok  bool
	Err Error
}

type DeleteReply struct {
	Ok  bool
	Err Error
}
