package pkg

func IsProtobuf(data []byte) bool {
	n := len(data)
	if n == 0 {
		return false
	}

	pos := 0
	tagCount := 0
	for pos < n {
		tag, vb := decodeVarint(data[pos:])
		if vb == 0 {
			return false
		}
		wireType := tag & 0x7
		fieldNum := tag >> 3
		if fieldNum == 0 || wireType > 5 {
			return false
		}
		pos += vb
		tagCount++

		switch wireType {
		case 0:
			_, vb := decodeVarint(data[pos:])
			if vb == 0 || pos+vb > n {
				return false
			}
			pos += vb
		case 1:
			if pos+8 > n {
				return false
			}
			pos += 8
		case 2:
			length, lb := decodeVarint(data[pos:])
			if lb == 0 || pos+lb+int(length) > n {
				return false
			}
			pos += lb + int(length)
		case 5:
			if pos+4 > n {
				return false
			}
			pos += 4
		default:
			return false
		}
	}

	return tagCount > 0 && pos == n
}

func decodeVarint(buf []byte) (uint64, int) {
	var x uint64
	var s uint
	for i, b := range buf {
		if i == 10 {
			return 0, 0
		}
		if b < 0x80 {
			return x | uint64(b)<<s, i + 1
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, 0
}
