package protocol

type EagerAllocationBufferProvider struct{}

func NewEagerAllocationBufferProvider() *EagerAllocationBufferProvider {
	return &EagerAllocationBufferProvider{}
}

func (p *EagerAllocationBufferProvider) Get(bufferSize int) ([]byte, error) {
	return make([]byte, bufferSize), nil
}

func (p *EagerAllocationBufferProvider) Put(buf []byte) {
	// Do nothing
}
