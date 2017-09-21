package barethoven

const base64Digits = `ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/`

func BareOSBase64(in []byte) string {
	var (
		reg, mask int32
		i         int
		rem       uint64
		buffer    string
	)
	for i < len(in) {
		if rem < 6 {
			reg <<= 8
			t := int32(in[i])
			i++
			t = twosComp(t, 8)
			reg |= t
			rem += 8
		}
		save := reg
		reg >>= (rem - 6)
		tmp := reg & 0x3F
		buffer += string(base64Digits[tmp])
		reg = save
		rem -= 6
	}
	mask = (1 << rem) - 1
	buffer += string(base64Digits[reg&mask])
	return buffer
}

func twosComp(val int32, bits uint32) int32 {
	if (val & (1 << (bits - 1))) != 0 {
		val = val - (1 << bits)
	}
	return val
}
