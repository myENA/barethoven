package barethoven

import (
	"crypto/hmac"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strings"
)

// crammit constructs a md5-cram response string to either be sent in response to a challenge or to validate an incoming
// challenge response
func crammit(client, password string, standardBase64 bool) string {
	var crammed string

	// encode password...
	pwh := md5.New()
	pwh.Write([]byte(password))
	pwHash := hex.EncodeToString(pwh.Sum(nil))

	// encode md5-cram challenge response...
	h := hmac.New(md5.New, []byte(pwHash))
	h.Write([]byte(client))
	ret := h.Sum(nil)

	// and finally base64 encode the resulting hash
	if standardBase64 {
		crammed = base64.RawStdEncoding.EncodeToString(ret)
	} else {
		crammed = BareOSBase64(ret)
	}

	return strings.TrimRight(crammed, "=")
}

func ntohl(buff []byte) int {
	var hostLength int
	var tmp uint32 = binary.BigEndian.Uint32(buff[0:4])

	if tmp > math.MaxInt32 {
		hostLength = int(tmp) - maxUint32Plus1
	} else {
		hostLength = int(tmp)
	}

	return hostLength
}

// bashSpaces replaces " " with "^A" as bareosApi uses this internally apparently.
func bashSpaces(in string) (out string) {
	split := strings.Split(in, "")

	for _, v := range split {
		if " " == v {
			out = fmt.Sprintf("%s%s", out, "^A")
		} else {
			out = fmt.Sprintf("%s%s", out, v)
		}
	}

	return
}

func rtrimNullBytes(in []byte) []byte {
	for i := len(in) - 1; i >= 0 && in[i] == 0; i-- {
		// Trim off null byte
		in = in[0:i]
	}
	return in
}
