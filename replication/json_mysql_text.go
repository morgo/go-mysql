package replication

import (
	"bytes"
	"math"
	"strconv"

	"github.com/goccy/go-json"
)

// This file holds the MySQL-text marshalers used by jsonBinaryDecoder
// when its mysqlTextMode flag is set. Wrapping the leaf decode returns
// in these types lets the existing json.Marshal pass produce JSON text
// that is faithful to each JSONB value's original type tag where the
// JSON text grammar can express it (DOUBLE 1.0 stays "1.0"; NEWDECIMAL
// stays unquoted; etc.) and preserves the JSONB key order.
//
// Caveats:
//   - Inter-token whitespace is compact (no space after ',' or ':'),
//     unlike MySQL's "SELECT json_col" form which puts a space after
//     both. DOUBLE scalars are byte-identical to MySQL's own rendering
//     (see jsonMySQLDouble / formatMySQLDouble for that contract);
//     other leaves are type-faithful as described above.
//   - NEWDECIMAL is the one tag that cannot be preserved on text
//     round-trip: MySQL's JSON text grammar has no decimal literal, so
//     re-inserting the unquoted number yields a JSON DOUBLE, not the
//     original JSONB_OPAQUE NEWDECIMAL. The numeric value still
//     round-trips; only the opaque type tag is lost. All other tags
//     covered here do reproduce the original JSONB binary on re-insert.

// jsonString carries a JSONB string payload as raw bytes so MarshalJSON
// can pass non-ASCII bytes through verbatim. MySQL JSON is byte-
// transparent, so bytes >= 0x20 (other than '"' and '\\') are written
// without UTF-8 validation -- unlike the default encoding/json path which
// replaces invalid UTF-8 with U+FFFD.
type jsonString string

func (s jsonString) MarshalJSON() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, len(s)+2))
	buf.WriteByte('"')
	writeJSONString(buf, []byte(s))
	buf.WriteByte('"')
	return buf.Bytes(), nil
}

// jsonRawNumber emits its bytes unquoted. Used for JSONB OPAQUE
// NEWDECIMAL values: MySQL renders these as plain numbers in JSON text,
// not as quoted strings.
type jsonRawNumber string

func (n jsonRawNumber) MarshalJSON() ([]byte, error) {
	return []byte(n), nil
}

// jsonMySQLDouble formats a float64 exactly the way MySQL 8.0 renders a
// JSON DOUBLE in JSON text. Byte-identity is the contract, not a
// cosmetic nicety: consumers write this text back into MySQL JSON
// columns, and MySQL's JSON text parser (rapidjson; see MySQL bugs
// #116160 and #112904) misrounds long fixed-point expansions by 1-2 ulp
// from roughly 1e25 upward. A formatter that spells such magnitudes in
// fixed notation (e.g. 'f'-formatting 1e308 into a 309-digit expansion)
// therefore corrupts the value when the text is re-inserted -- the
// binary round-trip through a JSON column is NOT safe for that regime.
// MySQL itself avoids the hazard by switching to scientific notation,
// so matching its rendering byte-for-byte both preserves values and
// keeps text-level checksums stable. See formatMySQLDouble for the
// derived format-selection rule.
type jsonMySQLDouble float64

func (f jsonMySQLDouble) MarshalJSON() ([]byte, error) {
	return []byte(formatMySQLDouble(float64(f))), nil
}

// jsonObject preserves JSONB key order (length-then-bytes, which is what
// MySQL emits) instead of going through map[string]any, which json.Marshal
// would sort lexicographically.
type jsonObject struct {
	keys   []string
	values []any
}

func (o jsonObject) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, k := range o.keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteByte('"')
		writeJSONString(&buf, []byte(k))
		buf.WriteString(`":`)
		vb, err := json.Marshal(o.values[i])
		if err != nil {
			return nil, err
		}
		buf.Write(vb)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// formatMySQLDouble returns the exact byte sequence MySQL 8.0 emits for
// a JSON DOUBLE scalar. The behaviour was derived empirically against
// MySQL 8.0.45 (via SELECT CAST(JSON_ARRAY(CAST(<str> AS DOUBLE)) AS
// CHAR), which bypasses the lossy rapidjson text parser) and validated
// byte-for-byte over ~25k doubles covering every decimal-exponent /
// digit-count combination near the format boundaries: 0 mismatches.
//
// The significant digits are always the shortest round-trip digit
// string, which Go's strconv with precision -1 already produces
// identically to MySQL's dtoa. Only the fixed-vs-scientific selection
// and the exponent spelling differ from Go's defaults:
//
//   - With decpt = the decimal point position relative to the
//     significant digits (f = 0.digits * 10^decpt) and nd = the number
//     of significant digits, MySQL uses fixed-point notation iff
//
//     decpt >= -14 && (decpt <= 15 || nd > decpt)
//
//     i.e. fixed-point while the decimal point sits in [-14, 15], plus
//     the one boundary case decpt == 16 && nd == 17 (a 17-significant-
//     digit value that still has a digit after the decimal point, e.g.
//     "1234567890123456.7"). nd <= 17 for every float64, so everything
//     with decpt >= 17 or decpt <= -15 is scientific. The sign plays no
//     part in the selection.
//
//   - Scientific notation spells the exponent without '+' and without
//     zero padding: MySQL writes "9.007199254740992e15" and "1.5e-5"
//     where Go's 'e'/'g' verbs write "9.007199254740992e+15" and
//     "1.5e-05".
//
//   - The JSON layer appends ".0" when the result contains neither '.'
//     nor 'e' (integral fixed-point values), so whole-number doubles
//     re-parse as JSON DOUBLE rather than JSON INTEGER: 1e14 renders as
//     "100000000000000.0" but 1e16 stays "1e16".
//
// NaN/Inf cannot be stored in MySQL JSON; they render as "null" rather
// than corrupt the surrounding document.
func formatMySQLDouble(f float64) string {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return "null"
	}
	// The 'e' form carries both drivers of the format selection: the
	// shortest round-trip digit string and the decimal exponent.
	sci := strconv.AppendFloat(make([]byte, 0, 32), f, 'e', -1, 64)
	ePos := bytes.IndexByte(sci, 'e')
	exp, err := strconv.Atoi(string(sci[ePos+1:]))
	if err != nil {
		// Unreachable: 'e'-formatted output always ends in "e±NN".
		return strconv.FormatFloat(f, 'g', -1, 64)
	}
	mant := sci[:ePos] // "d" or "d.ddd", optionally '-'-prefixed
	neg := mant[0] == '-'
	if neg {
		mant = mant[1:]
	}
	nd := len(mant)
	if nd > 1 {
		nd-- // drop the '.' at mant[1]
	}
	decpt := exp + 1 // f = 0.digits * 10^decpt

	if decpt >= -14 && (decpt <= 15 || nd > decpt) {
		// Fixed-point window. Longest possible form is 34 bytes
		// (sign + "0." + 14 zeros + 17 digits).
		out := strconv.AppendFloat(make([]byte, 0, 40), f, 'f', -1, 64)
		if bytes.IndexByte(out, '.') < 0 {
			out = append(out, '.', '0')
		}
		return string(out)
	}

	// Scientific notation: reuse Go's mantissa, respell the exponent.
	out := make([]byte, 0, len(sci))
	if neg {
		out = append(out, '-')
	}
	out = append(out, mant...)
	out = append(out, 'e')
	out = strconv.AppendInt(out, int64(exp), 10)
	return string(out)
}

// writeJSONString writes s as the contents of a JSON string (no
// surrounding quotes), with byte-transparent semantics: bytes >= 0x20
// other than '"' and '\\' are written verbatim, including high-bit bytes
// that may not form valid UTF-8.
func writeJSONString(buf *bytes.Buffer, s []byte) {
	const hexdigits = "0123456789abcdef"
	for i := range len(s) {
		c := s[i]
		if c < 0x20 || c == '"' || c == '\\' {
			switch c {
			case '"':
				buf.WriteString(`\"`)
			case '\\':
				buf.WriteString(`\\`)
			case '\b':
				buf.WriteString(`\b`)
			case '\f':
				buf.WriteString(`\f`)
			case '\n':
				buf.WriteString(`\n`)
			case '\r':
				buf.WriteString(`\r`)
			case '\t':
				buf.WriteString(`\t`)
			default:
				buf.WriteString(`\u00`)
				buf.WriteByte(hexdigits[c>>4])
				buf.WriteByte(hexdigits[c&0xF])
			}
			continue
		}
		buf.WriteByte(c)
	}
}
