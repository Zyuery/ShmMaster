package fixed

import (
	"fmt"
	"reflect"
	"unsafe"
)

// Storager 供 SetFixed/GetFixed 使用的存储接口。
type Storager interface {
	Set(key string, value []byte) error
	Get(key string) ([]byte, bool, error)
}

func assertNoPointers[T any]() error {
	var zero T
	return typeNoPointers(reflect.TypeOf(zero))
}

func typeNoPointers(t reflect.Type) error {
	switch t.Kind() {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64:
		return nil
	case reflect.Array:
		return typeNoPointers(t.Elem())
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if err := typeNoPointers(t.Field(i).Type); err != nil {
				return fmt.Errorf("field %s: %w", t.Field(i).Name, err)
			}
		}
		return nil
	case reflect.String, reflect.Slice, reflect.Map, reflect.Pointer,
		reflect.Interface, reflect.Func, reflect.Chan, reflect.UnsafePointer:
		return fmt.Errorf("type %s contains pointer-like data", t.String())
	default:
		return fmt.Errorf("unsupported kind %s (%s)", t.Kind(), t.String())
	}
}

func bytesViewOf[T any](p *T) []byte {
	n := int(unsafe.Sizeof(*p))
	return unsafe.Slice((*byte)(unsafe.Pointer(p)), n)
}

// SetFixed 将无指针类型 T 的实例写入 db。
func SetFixed[T any](db Storager, key string, v *T) error {
	if err := assertNoPointers[T](); err != nil {
		return err
	}
	return db.Set(key, bytesViewOf(v))
}

// GetFixed 从 db 读出并为 *T。
func GetFixed[T any](db Storager, key string) (*T, bool, error) {
	if err := assertNoPointers[T](); err != nil {
		return nil, false, err
	}
	b, ok, err := db.Get(key)
	if err != nil || !ok {
		return nil, ok, err
	}
	want := int(unsafe.Sizeof(*new(T)))
	if len(b) != want {
		return nil, false, fmt.Errorf("size mismatch: got=%d want=%d", len(b), want)
	}
	return (*T)(unsafe.Pointer(&b[0])), true, nil
}
