package segment

import (
	"encoding/binary"
	"fmt"
)

// File format constants.
const (
	Magic      = "S3S1"
	Version    = uint32(1)
	FooterSize = 128
)

// Footer is the fixed-size tail of a .seg file.
// All offsets are absolute from start of file.
type Footer struct {
	PostingsOffset  uint64
	PostingsLen     uint64
	PositionsOffset uint64
	PositionsLen    uint64
	DocstoreOffset  uint64
	DocstoreLen     uint64
	DoclensOffset   uint64
	DoclensLen      uint64
	DocCount        uint32
}

// layout: 8x uint64 (64 bytes) + uint32 DocCount (4) + pad (56 bytes) + uint32 Version (4) + [4]byte Magic
// total = 64+4+56+4+4 = 132 — but we want 128. Adjust pad:
// 8x8=64, DocCount=4, Version=4, Magic=4 → content=76 → pad=128-76=52
const footerPad = FooterSize - 8*8 - 4 - 4 - 4 // = 52

// EncodeFooter serializes Footer into exactly FooterSize bytes.
func EncodeFooter(f Footer) []byte {
	buf := make([]byte, FooterSize)
	off := 0
	putU64 := func(v uint64) { binary.LittleEndian.PutUint64(buf[off:], v); off += 8 }
	putU32 := func(v uint32) { binary.LittleEndian.PutUint32(buf[off:], v); off += 4 }
	putU64(f.PostingsOffset)
	putU64(f.PostingsLen)
	putU64(f.PositionsOffset)
	putU64(f.PositionsLen)
	putU64(f.DocstoreOffset)
	putU64(f.DocstoreLen)
	putU64(f.DoclensOffset)
	putU64(f.DoclensLen)
	putU32(f.DocCount)
	// pad: off is now 8*8+4 = 68; pad to FooterSize-8
	off = FooterSize - 8
	putU32(Version)
	copy(buf[FooterSize-4:], []byte(Magic))
	return buf
}

// DecodeFooter parses the last FooterSize bytes of a .seg file.
func DecodeFooter(data []byte) (Footer, error) {
	if len(data) < FooterSize {
		return Footer{}, fmt.Errorf("segment: footer data too short")
	}
	tail := data[len(data)-FooterSize:]
	if string(tail[FooterSize-4:]) != Magic {
		return Footer{}, fmt.Errorf("segment: bad footer magic")
	}
	off := 0
	getU64 := func() uint64 { v := binary.LittleEndian.Uint64(tail[off:]); off += 8; return v }
	getU32 := func() uint32 { v := binary.LittleEndian.Uint32(tail[off:]); off += 4; return v }
	var f Footer
	f.PostingsOffset = getU64()
	f.PostingsLen = getU64()
	f.PositionsOffset = getU64()
	f.PositionsLen = getU64()
	f.DocstoreOffset = getU64()
	f.DocstoreLen = getU64()
	f.DoclensOffset = getU64()
	f.DoclensLen = getU64()
	f.DocCount = getU32()
	return f, nil
}
