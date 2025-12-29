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
	Records []Record
	Err     Error
}

type TrimReply struct {
	Ok  bool
	Err Error
}

type DeleteReply struct {
	Ok  bool
	Err Error
}
