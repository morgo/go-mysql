package replication

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"math"
	"strconv"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// jsonbVisitor receives events as a JSONB byte stream is walked. The walk
// itself lives in jsonbWalker (json_binary.go); each leaf or container
// boundary translates into one call. Two visitor implementations consume
// these events: goValueVisitor builds the Go value tree the legacy path
// hands to json.Marshal; mysqlTextVisitor writes MySQL's textual JSON
// form directly to a buffer.
type jsonbVisitor interface {
	BeginObject(count int)
	EndObject()
	BeginArray(count int)
	EndArray()
	// BeforeEntry is called before each non-first sibling inside a
	// container, so a visitor can emit a separator.
	BeforeEntry()
	// Key is called before each value in an object, after BeforeEntry
	// when applicable.
	Key(key []byte)

	Null()
	Bool(b bool)
	Int(v int64)
	Uint(v uint64)
	Double(v float64)
	String(s []byte)

	Decimal(precision, scale int, payload []byte)
	Time(payload []byte)
	Date(payload []byte)
	DateTime(payload []byte)
	OpaqueUnknown(tp byte, payload []byte)
}

// goValueVisitor builds a Go value tree (map[string]any / []any / scalars)
// that mirrors the original decoder output. Used by the legacy
// json.Marshal path.
type goValueVisitor struct {
	useDecimal               bool
	useFloatWithTrailingZero bool

	stack []goContainer
	root  any
	err   error
}

type goContainer struct {
	isObject   bool
	pendingKey string
	keys       []string
	values     []any
}

func (g *goValueVisitor) attach(v any) {
	if g.err != nil {
		return
	}
	if len(g.stack) == 0 {
		g.root = v
		return
	}
	top := &g.stack[len(g.stack)-1]
	if top.isObject {
		top.keys = append(top.keys, top.pendingKey)
	}
	top.values = append(top.values, v)
}

func (g *goValueVisitor) BeginObject(count int) {
	g.stack = append(g.stack, goContainer{
		isObject: true,
		keys:     make([]string, 0, count),
		values:   make([]any, 0, count),
	})
}

func (g *goValueVisitor) EndObject() {
	top := g.stack[len(g.stack)-1]
	g.stack = g.stack[:len(g.stack)-1]
	m := make(map[string]any, len(top.keys))
	for i, k := range top.keys {
		m[k] = top.values[i]
	}
	g.attach(m)
}

func (g *goValueVisitor) BeginArray(count int) {
	g.stack = append(g.stack, goContainer{
		values: make([]any, 0, count),
	})
}

func (g *goValueVisitor) EndArray() {
	top := g.stack[len(g.stack)-1]
	g.stack = g.stack[:len(g.stack)-1]
	g.attach(top.values)
}

func (g *goValueVisitor) BeforeEntry() {}

func (g *goValueVisitor) Key(k []byte) {
	g.stack[len(g.stack)-1].pendingKey = string(k)
}

func (g *goValueVisitor) Null()         { g.attach(nil) }
func (g *goValueVisitor) Bool(b bool)   { g.attach(b) }
func (g *goValueVisitor) Int(v int64)   { g.attach(v) }
func (g *goValueVisitor) Uint(v uint64) { g.attach(v) }

func (g *goValueVisitor) Double(v float64) {
	if g.useFloatWithTrailingZero {
		g.attach(FloatWithTrailingZero(v))
		return
	}
	g.attach(v)
}

func (g *goValueVisitor) String(s []byte) { g.attach(string(s)) }

func (g *goValueVisitor) Decimal(prec, scale int, payload []byte) {
	v, _, err := decodeDecimal(payload, prec, scale, g.useDecimal)
	if err != nil {
		g.err = err
		return
	}
	g.attach(v)
}

func (g *goValueVisitor) Time(payload []byte) {
	g.attach(formatJSONTime(payload))
}

func (g *goValueVisitor) Date(payload []byte) {
	// Legacy behaviour: DATE values come back through the same formatter
	// as DATETIME, so they include " 00:00:00.000000". Preserved as-is to
	// avoid breaking existing consumers.
	g.attach(formatJSONDateTime(payload, false))
}

func (g *goValueVisitor) DateTime(payload []byte) {
	g.attach(formatJSONDateTime(payload, false))
}

func (g *goValueVisitor) OpaqueUnknown(_ byte, payload []byte) {
	g.attach(string(payload))
}

// mysqlTextVisitor writes MySQL's textual JSON form (the representation
// "SELECT json_col" returns) directly to a buffer. Each JSONB value's
// original type tag survives unchanged -- DOUBLE 1.0 stays "1.0",
// NEWDECIMAL stays unquoted, etc.
type mysqlTextVisitor struct {
	buf bytes.Buffer
	err error
}

func (m *mysqlTextVisitor) BeginObject(_ int) { m.buf.WriteByte('{') }
func (m *mysqlTextVisitor) EndObject()        { m.buf.WriteByte('}') }
func (m *mysqlTextVisitor) BeginArray(_ int)  { m.buf.WriteByte('[') }
func (m *mysqlTextVisitor) EndArray()         { m.buf.WriteByte(']') }
func (m *mysqlTextVisitor) BeforeEntry()      { m.buf.WriteString(", ") }

func (m *mysqlTextVisitor) Key(k []byte) {
	m.buf.WriteByte('"')
	writeJSONString(&m.buf, k)
	m.buf.WriteString(`": `)
}

func (m *mysqlTextVisitor) Null() { m.buf.WriteString("null") }

func (m *mysqlTextVisitor) Bool(b bool) {
	if b {
		m.buf.WriteString("true")
	} else {
		m.buf.WriteString("false")
	}
}

func (m *mysqlTextVisitor) Int(v int64)   { m.buf.WriteString(strconv.FormatInt(v, 10)) }
func (m *mysqlTextVisitor) Uint(v uint64) { m.buf.WriteString(strconv.FormatUint(v, 10)) }
func (m *mysqlTextVisitor) Double(v float64) {
	m.buf.WriteString(formatMySQLDouble(v))
}

func (m *mysqlTextVisitor) String(s []byte) {
	m.buf.WriteByte('"')
	writeJSONString(&m.buf, s)
	m.buf.WriteByte('"')
}

func (m *mysqlTextVisitor) Decimal(prec, scale int, payload []byte) {
	v, _, err := decodeDecimal(payload, prec, scale, false)
	if err != nil {
		m.err = err
		return
	}
	s, _ := v.(string)
	m.buf.WriteString(s)
}

func (m *mysqlTextVisitor) Time(payload []byte) {
	m.buf.WriteByte('"')
	m.buf.WriteString(formatJSONTime(payload))
	m.buf.WriteByte('"')
}

func (m *mysqlTextVisitor) Date(payload []byte) {
	m.buf.WriteByte('"')
	m.buf.WriteString(formatJSONDateTime(payload, true))
	m.buf.WriteByte('"')
}

func (m *mysqlTextVisitor) DateTime(payload []byte) {
	m.buf.WriteByte('"')
	m.buf.WriteString(formatJSONDateTime(payload, false))
	m.buf.WriteByte('"')
}

func (m *mysqlTextVisitor) OpaqueUnknown(tp byte, payload []byte) {
	m.buf.WriteByte('"')
	m.buf.WriteString("base64:type")
	m.buf.WriteString(strconv.Itoa(int(tp)))
	m.buf.WriteByte(':')
	m.buf.WriteString(base64.StdEncoding.EncodeToString(payload))
	m.buf.WriteByte('"')
}

// formatMySQLDouble formats a float64 the way MySQL does in JSON text:
// whole-number doubles keep a trailing ".0", and other values use the
// shortest round-trippable form.
func formatMySQLDouble(f float64) string {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return "null"
	}
	if f == math.Trunc(f) {
		return strconv.FormatFloat(f, 'f', 1, 64)
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// writeJSONString writes s as the contents of a JSON string (no
// surrounding quotes). MySQL JSON is byte-transparent, so bytes >= 0x20
// other than '"' and '\\' are written verbatim (including high-bit bytes
// that may not form valid UTF-8).
func writeJSONString(buf *bytes.Buffer, s []byte) {
	const hexdigits = "0123456789abcdef"
	for i := 0; i < len(s); i++ {
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

func formatJSONTime(payload []byte) string {
	if len(payload) < 8 {
		return "00:00:00"
	}
	v := mysql.ParseBinaryInt64(payload[:8])
	if v == 0 {
		return "00:00:00"
	}
	sign := ""
	if v < 0 {
		sign = "-"
		v = -v
	}
	intPart := v >> 24
	hour := (intPart >> 12) % (1 << 10)
	minute := (intPart >> 6) % (1 << 6)
	sec := intPart % (1 << 6)
	frac := v % (1 << 24)
	return fmt.Sprintf("%s%02d:%02d:%02d.%06d", sign, hour, minute, sec, frac)
}

func formatJSONDateTime(payload []byte, isDate bool) string {
	if len(payload) < 8 {
		if isDate {
			return "0000-00-00"
		}
		return "0000-00-00 00:00:00"
	}
	v := mysql.ParseBinaryInt64(payload[:8])
	if v == 0 {
		if isDate {
			return "0000-00-00"
		}
		return "0000-00-00 00:00:00"
	}
	if v < 0 {
		v = -v
	}
	intPart := v >> 24
	ymd := intPart >> 17
	ym := ymd >> 5
	hms := intPart % (1 << 17)
	year := ym / 13
	month := ym % 13
	day := ymd % (1 << 5)
	hour := hms >> 12
	minute := (hms >> 6) % (1 << 6)
	second := hms % (1 << 6)
	frac := v % (1 << 24)
	if isDate {
		return fmt.Sprintf("%04d-%02d-%02d", year, month, day)
	}
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d", year, month, day, hour, minute, second, frac)
}
