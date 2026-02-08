package core

import (
	"fmt"
	"reflect"
	"unsafe"
)

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
	// 这些都可能携带指针 / 运行时对象
	case reflect.String, reflect.Slice, reflect.Map, reflect.Pointer,
		reflect.Interface, reflect.Func, reflect.Chan, reflect.UnsafePointer:
		return fmt.Errorf("type %s contains pointer-like data", t.String())
	default:
		return fmt.Errorf("unsupported kind %s (%s)", t.Kind(), t.String())
	}
}

// -------- struct <-> []byte（memcpy 语义） --------
func bytesViewOf[T any](p *T) []byte {
	// 1. 获取指针指向的变量占用的内存字节数
	// unsafe.Sizeof(*p)：计算 *p（即指针指向的实际变量）的内存大小，返回 uintptr 类型
	// int()：转换为 int 类型，因为切片长度需要 int 类型
	n := int(unsafe.Sizeof(*p))

	// 2. 核心转换逻辑
	// unsafe.Pointer(p)：将类型化指针 *T 转换为通用的无类型指针
	// (*byte)(...)：将无类型指针强制转换为 *byte 类型指针（字节指针）
	// unsafe.Slice：基于字节指针和长度 n，创建一个字节切片
	// 这个切片是原变量内存的"视图"，修改切片会直接修改原变量的内存
	return unsafe.Slice((*byte)(unsafe.Pointer(p)), n)
}

func SetFixed[T any](db *DB, key string, v *T) error {
	if err := assertNoPointers[T](); err != nil {
		return err
	}
	return db.Set(key, bytesViewOf(v))
}

func GetFixed[T any](db *DB, key string) (*T, bool, error) {
	if err := assertNoPointers[T](); err != nil {
		return nil, false, err
	}
	b, ok, err := db.Get(key)
	if err != nil || !ok {
		return nil, ok, err
	}
	out := new(T)
	want := int(unsafe.Sizeof(*out))
	if len(b) != want {
		return nil, false, fmt.Errorf("size mismatch: got=%d want=%d", len(b), want)
	}

	copy(bytesViewOf(out), b)
	return out, true, nil
}
