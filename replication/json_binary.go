package replication

import (
	"fmt"
	"math"
	"strconv"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/goccy/go-json"
	"github.com/pingcap/errors"
)

//nolint:revive // JSONB type tags mirror the upstream MySQL JSON binary format
const (
	JSONB_SMALL_OBJECT byte = iota // small JSON object
	JSONB_LARGE_OBJECT             // large JSON object
	JSONB_SMALL_ARRAY              // small JSON array
	JSONB_LARGE_ARRAY              // large JSON array
	JSONB_LITERAL                  // literal (true/false/null)
	JSONB_INT16                    // int16
	JSONB_UINT16                   // uint16
	JSONB_INT32                    // int32
	JSONB_UINT32                   // uint32
	JSONB_INT64                    // int64
	JSONB_UINT64                   // uint64
	JSONB_DOUBLE                   // double
	JSONB_STRING                   // string
	JSONB_OPAQUE       byte = 0x0f // custom data (any MySQL data type)
)

//nolint:revive // JSONB literal tags mirror the upstream MySQL JSON binary format
const (
	JSONB_NULL_LITERAL  byte = 0x00
	JSONB_TRUE_LITERAL  byte = 0x01
	JSONB_FALSE_LITERAL byte = 0x02
)

const (
	jsonbSmallOffsetSize = 2
	jsonbLargeOffsetSize = 4

	jsonbKeyEntrySizeSmall = 2 + jsonbSmallOffsetSize
	jsonbKeyEntrySizeLarge = 2 + jsonbLargeOffsetSize

	jsonbValueEntrySizeSmall = 1 + jsonbSmallOffsetSize
	jsonbValueEntrySizeLarge = 1 + jsonbLargeOffsetSize
)

var ErrCorruptedJSONDiff = fmt.Errorf("corrupted JSON diff") // ER_CORRUPTED_JSON_DIFF

//nolint:revive // exported type renamed would be a breaking API change
type (
	// JsonDiffOperation is an enum that describes what kind of operation a JsonDiff object represents.
	// https://github.com/mysql/mysql-server/blob/8.0/sql/json_diff.h
	JsonDiffOperation byte
)

type FloatWithTrailingZero float64

//nolint:revive // exported constants renamed would be a breaking API change
const (
	// The JSON value in the given path is replaced with a new value.
	//
	// It has the same effect as `JSON_REPLACE(col, path, value)`.
	JsonDiffOperationReplace = JsonDiffOperation(iota)

	// Add a new element at the given path.
	//
	//  If the path specifies an array element, it has the same effect as `JSON_ARRAY_INSERT(col, path, value)`.
	//
	//  If the path specifies an object member, it has the same effect as `JSON_INSERT(col, path, value)`.
	JsonDiffOperationInsert

	// The JSON value at the given path is removed from an array or object.
	//
	// It has the same effect as `JSON_REMOVE(col, path)`.
	JsonDiffOperationRemove
)

//nolint:revive // exported type renamed would be a breaking API change
type (
	JsonDiff struct {
		Op    JsonDiffOperation
		Path  string
		Value string
	}
)

func (op JsonDiffOperation) String() string {
	switch op {
	case JsonDiffOperationReplace:
		return "Replace"
	case JsonDiffOperationInsert:
		return "Insert"
	case JsonDiffOperationRemove:
		return "Remove"
	default:
		return fmt.Sprintf("Unknown(%d)", op)
	}
}

func (jd *JsonDiff) String() string {
	return fmt.Sprintf("json_diff(op:%s path:%s value:%s)", jd.Op, jd.Path, jd.Value)
}

func (f FloatWithTrailingZero) MarshalJSON() ([]byte, error) {
	if float64(f) == float64(int(f)) {
		return []byte(strconv.FormatFloat(float64(f), 'f', 1, 64)), nil
	}

	return []byte(strconv.FormatFloat(float64(f), 'f', -1, 64)), nil
}

func jsonbGetOffsetSize(isSmall bool) int {
	if isSmall {
		return jsonbSmallOffsetSize
	}

	return jsonbLargeOffsetSize
}

func jsonbGetKeyEntrySize(isSmall bool) int {
	if isSmall {
		return jsonbKeyEntrySizeSmall
	}

	return jsonbKeyEntrySizeLarge
}

func jsonbGetValueEntrySize(isSmall bool) int {
	if isSmall {
		return jsonbValueEntrySizeSmall
	}

	return jsonbValueEntrySizeLarge
}

// decodeJSONBinary decodes the JSON binary encoding data and returns the
// common JSON encoding data. The JSONB byte stream is walked once and
// dispatched to a jsonbVisitor: goValueVisitor for the legacy path
// (build a Go value tree, then json.Marshal it) and mysqlTextVisitor when
// RenderJSONAsMySQLText is set on the parent RowsEvent (write MySQL's
// textual JSON form directly, preserving each value's original type tag).
func (e *RowsEvent) decodeJSONBinary(data []byte) ([]byte, error) {
	if len(data) < 1 {
		if e.renderJSONAsMySQLText && e.ignoreJSONDecodeErr {
			return []byte("null"), nil
		}
		return nil, errors.New("json binary data is empty")
	}

	if e.renderJSONAsMySQLText {
		v := &mysqlTextVisitor{}
		w := jsonbWalker{visitor: v, ignoreDecodeErr: e.ignoreJSONDecodeErr}
		w.walkValue(data[0], data[1:])
		if w.err != nil {
			return nil, w.err
		}
		if v.err != nil {
			return nil, v.err
		}
		return v.buf.Bytes(), nil
	}

	v := &goValueVisitor{
		useDecimal:               e.useDecimal,
		useFloatWithTrailingZero: e.useFloatWithTrailingZero,
	}
	w := jsonbWalker{visitor: v, ignoreDecodeErr: e.ignoreJSONDecodeErr}
	w.walkValue(data[0], data[1:])
	if w.err != nil {
		return nil, w.err
	}
	if v.err != nil {
		return nil, v.err
	}
	return json.Marshal(v.root)
}

// jsonbWalker walks a JSONB byte stream and emits events to its visitor.
// It owns all structural parsing (type-tag dispatch, offset tables,
// inline-vs-pointer values, opaque-payload inner types) so visitors only
// see semantic events.
type jsonbWalker struct {
	visitor         jsonbVisitor
	ignoreDecodeErr bool
	err             error
}

func (w *jsonbWalker) walkValue(tp byte, data []byte) {
	if w.err != nil {
		return
	}

	switch tp {
	case JSONB_SMALL_OBJECT:
		w.walkObjectOrArray(data, true, true)
	case JSONB_LARGE_OBJECT:
		w.walkObjectOrArray(data, false, true)
	case JSONB_SMALL_ARRAY:
		w.walkObjectOrArray(data, true, false)
	case JSONB_LARGE_ARRAY:
		w.walkObjectOrArray(data, false, false)
	case JSONB_LITERAL:
		w.walkLiteral(data)
	case JSONB_INT16:
		if !w.requireLen(data, 2) {
			return
		}
		w.visitor.Int(int64(mysql.ParseBinaryInt16(data[:2])))
	case JSONB_UINT16:
		if !w.requireLen(data, 2) {
			return
		}
		w.visitor.Uint(uint64(mysql.ParseBinaryUint16(data[:2])))
	case JSONB_INT32:
		if !w.requireLen(data, 4) {
			return
		}
		w.visitor.Int(int64(mysql.ParseBinaryInt32(data[:4])))
	case JSONB_UINT32:
		if !w.requireLen(data, 4) {
			return
		}
		w.visitor.Uint(uint64(mysql.ParseBinaryUint32(data[:4])))
	case JSONB_INT64:
		if !w.requireLen(data, 8) {
			return
		}
		w.visitor.Int(mysql.ParseBinaryInt64(data[:8]))
	case JSONB_UINT64:
		if !w.requireLen(data, 8) {
			return
		}
		w.visitor.Uint(mysql.ParseBinaryUint64(data[:8]))
	case JSONB_DOUBLE:
		if !w.requireLen(data, 8) {
			return
		}
		w.visitor.Double(mysql.ParseBinaryFloat64(data[:8]))
	case JSONB_STRING:
		w.walkString(data)
	case JSONB_OPAQUE:
		w.walkOpaque(data)
	default:
		w.err = errors.Errorf("invalid json type %d", tp)
	}
}

func (w *jsonbWalker) walkLiteral(data []byte) {
	if !w.requireLen(data, 1) {
		return
	}
	switch data[0] {
	case JSONB_NULL_LITERAL:
		w.visitor.Null()
	case JSONB_TRUE_LITERAL:
		w.visitor.Bool(true)
	case JSONB_FALSE_LITERAL:
		w.visitor.Bool(false)
	default:
		w.err = errors.Errorf("invalid literal %c", data[0])
	}
}

func (w *jsonbWalker) walkObjectOrArray(data []byte, isSmall, isObject bool) {
	offsetSize := jsonbGetOffsetSize(isSmall)
	if !w.requireLen(data, 2*offsetSize) {
		return
	}

	count := readJSONCount(data, isSmall)
	size := readJSONCount(data[offsetSize:], isSmall)

	if len(data) < size {
		// Before MySQL 5.7.22, json type generated column may have invalid
		// value (bug ref: https://bugs.mysql.com/bug.php?id=88791). The
		// generated column value is not used in replication, so we can
		// emit null in its place when ignoreDecodeErr is set.
		if w.ignoreDecodeErr {
			w.visitor.Null()
			return
		}
		w.err = errors.Errorf("data len %d < expected %d", len(data), size)
		return
	}

	keyEntrySize := jsonbGetKeyEntrySize(isSmall)
	valueEntrySize := jsonbGetValueEntrySize(isSmall)
	headerSize := 2*offsetSize + count*valueEntrySize
	if isObject {
		headerSize += count * keyEntrySize
	}
	if headerSize > size {
		w.err = errors.Errorf("header size %d > size %d", headerSize, size)
		return
	}

	var keys [][]byte
	if isObject {
		keys = make([][]byte, count)
		for i := 0; i < count; i++ {
			entryOffset := 2*offsetSize + keyEntrySize*i
			keyOffset := readJSONCount(data[entryOffset:], isSmall)
			keyLength := int(mysql.ParseBinaryUint16(data[entryOffset+offsetSize : entryOffset+offsetSize+2]))
			if keyOffset < headerSize {
				w.err = errors.Errorf("invalid key offset %d, must >= %d", keyOffset, headerSize)
				return
			}
			if !w.requireLen(data, keyOffset+keyLength) {
				return
			}
			keys[i] = data[keyOffset : keyOffset+keyLength]
		}
	}

	if isObject {
		w.visitor.BeginObject(count)
	} else {
		w.visitor.BeginArray(count)
	}

	for i := 0; i < count; i++ {
		if i > 0 {
			w.visitor.BeforeEntry()
		}
		if isObject {
			w.visitor.Key(keys[i])
		}

		entryOffset := 2*offsetSize + valueEntrySize*i
		if isObject {
			entryOffset += keyEntrySize * count
		}
		tp := data[entryOffset]
		if isInlineValue(tp, isSmall) {
			w.walkValue(tp, data[entryOffset+1:entryOffset+valueEntrySize])
		} else {
			valueOffset := readJSONCount(data[entryOffset+1:], isSmall)
			if !w.requireLen(data, valueOffset) {
				return
			}
			w.walkValue(tp, data[valueOffset:])
		}
		if w.err != nil {
			return
		}
	}

	if isObject {
		w.visitor.EndObject()
	} else {
		w.visitor.EndArray()
	}
}

func isInlineValue(tp byte, isSmall bool) bool {
	switch tp {
	case JSONB_INT16, JSONB_UINT16, JSONB_LITERAL:
		return true
	case JSONB_INT32, JSONB_UINT32:
		return !isSmall
	}
	return false
}

func (w *jsonbWalker) walkString(data []byte) {
	l, n, err := readJSONVarLen(data)
	if err != nil {
		w.err = err
		return
	}
	if !w.requireLen(data, l+n) {
		return
	}
	w.visitor.String(data[n : n+l])
}

func (w *jsonbWalker) walkOpaque(data []byte) {
	if !w.requireLen(data, 1) {
		return
	}
	tp := data[0]
	data = data[1:]
	l, n, err := readJSONVarLen(data)
	if err != nil {
		w.err = err
		return
	}
	if !w.requireLen(data, l+n) {
		return
	}
	payload := data[n : n+l]
	switch tp {
	case mysql.MYSQL_TYPE_NEWDECIMAL:
		if len(payload) < 2 {
			w.err = errors.Errorf("decimal payload too short: %d", len(payload))
			return
		}
		w.visitor.Decimal(int(payload[0]), int(payload[1]), payload[2:])
	case mysql.MYSQL_TYPE_TIME:
		w.visitor.Time(payload)
	case mysql.MYSQL_TYPE_DATE:
		w.visitor.Date(payload)
	case mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_TIMESTAMP:
		w.visitor.DateTime(payload)
	default:
		w.visitor.OpaqueUnknown(tp, payload)
	}
}

func (w *jsonbWalker) requireLen(data []byte, expected int) bool {
	if len(data) < expected {
		w.err = errors.Errorf("data len %d < expected %d", len(data), expected)
		return false
	}
	return true
}

func readJSONCount(data []byte, isSmall bool) int {
	if isSmall {
		return int(mysql.ParseBinaryUint16(data[:2]))
	}
	return int(mysql.ParseBinaryUint32(data[:4]))
}

// readJSONVarLen decodes the variable-length integer used by JSONB.
func readJSONVarLen(data []byte) (length, consumed int, err error) {
	maxCount := len(data)
	if maxCount > 5 {
		maxCount = 5
	}
	var l uint64
	for pos := 0; pos < maxCount; pos++ {
		v := data[pos]
		l |= uint64(v&0x7F) << uint(7*pos)
		if v&0x80 == 0 {
			if l > math.MaxUint32 {
				return 0, 0, errors.Errorf("variable length %d exceeds %d", l, int64(math.MaxUint32))
			}
			return int(l), pos + 1, nil
		}
	}
	return 0, 0, errors.New("decode variable length failed")
}

func (e *RowsEvent) decodeJSONPartialBinary(data []byte) (*JsonDiff, error) {
	// see Json_diff_vector::read_binary() in mysql-server/sql/json_diff.cc
	operationNumber := JsonDiffOperation(data[0])
	switch operationNumber {
	case JsonDiffOperationReplace:
	case JsonDiffOperationInsert:
	case JsonDiffOperationRemove:
	default:
		return nil, ErrCorruptedJSONDiff
	}
	data = data[1:]

	pathLength, _, n := mysql.LengthEncodedInt(data)
	data = data[n:]

	path := data[:pathLength]
	data = data[pathLength:]

	diff := &JsonDiff{
		Op:   operationNumber,
		Path: string(path),
		// Value will be filled below
	}

	if operationNumber == JsonDiffOperationRemove {
		return diff, nil
	}

	valueLength, _, n := mysql.LengthEncodedInt(data)
	data = data[n:]

	d, err := e.decodeJSONBinary(data[:valueLength])
	if err != nil {
		return nil, fmt.Errorf("cannot read json diff for field %q: %w", path, err)
	}
	diff.Value = string(d)

	return diff, nil
}
