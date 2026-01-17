package log_old

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

type TestRecord struct {
	Offset    int64
	Timestamp int64
	Data      []byte
}

func NewTestRecord(offset int64, data []byte) *TestRecord {
	return &TestRecord{
		Offset:    offset,
		Timestamp: 0, // TODO: set timestamp
		Data:      data,
	}
}

func (r TestRecord) BinaryLength() int {
	return 4 + 8 + 4 + len(r.Data) // CRC + offset + timestamp + data
}

func (r TestRecord) MarshalBinary(buf []byte) (int, error) {
	binLength := r.BinaryLength()
	totalLen := 4 + r.BinaryLength() // length + binary length

	// Write record binary length prefix
	binary.BigEndian.PutUint32(buf[0:4], uint32(binLength))

	// Write offset, timestamp, data (for CRC calculation)
	pos := 8 // After length and CRC placeholder (skipping CRC for now by jumping 8 at once)
	binary.BigEndian.PutUint64(buf[pos:], uint64(r.Offset))
	binary.BigEndian.PutUint64(buf[pos+8:], uint64(r.Timestamp))
	copy(buf[pos+16:], r.Data)

	// Compute and write CRC of [offset][timestamp][data]
	crc := crc32.ChecksumIEEE(buf[8 : 8+binLength])
	binary.BigEndian.PutUint32(buf[4:8], crc)

	return totalLen, nil
}

func TestUnmarshalBinary(r io.Reader) (TestRecord, error) {
	var length uint32
	err := binary.Read(r, binary.BigEndian, &length)
	if err != nil {
		return TestRecord{}, err
	}

	recordData := make([]byte, length)
	_, err = io.ReadFull(r, recordData)
	if err != nil {
		return TestRecord{}, err
	}

	storedCRC := binary.BigEndian.Uint32(recordData[0:4])
	computedCRC := crc32.ChecksumIEEE(recordData[4:])
	if storedCRC != computedCRC {
		return TestRecord{}, fmt.Errorf("corrupted record")
	}

	return TestRecord{
		Offset:    int64(binary.BigEndian.Uint64(recordData[4:12])),
		Timestamp: int64(binary.BigEndian.Uint32(recordData[12:16])),
		Data:      recordData[16:],
	}, nil
}
