package logger

import (
	"reflect"
	"testing"

	"go.uber.org/zap/zapcore"
)

// Вспомогательные структуры для тестов
type Inner struct {
	Login    string `log:"true"`
	Password string
	Hidden   string
}
type Outer struct {
	ID      int                    `log:"true"`
	Name    string                 `log:"true"`
	Inner   Inner                  `log:"true"`
	PInner  *Inner                 `log:"true"`
	Numbers []int                  `log:"true"`
	M       map[string]interface{} `log:"true"`
	PtrNil  *int                   `log:"true"`
	Ch      chan int               `log:"true"` // неподдерживаемый тип -> ***SKIPPED***
	private string                 // неэкспортируемое поле -> пропускается
}

func encode(t *testing.T, v interface{}) (*zapcore.MapObjectEncoder, error) {
	t.Helper()
	enc := zapcore.NewMapObjectEncoder()
	err := MarshalSanitizedObject(v, enc)
	return enc, err
}

func TestMarshalSanitizedObject_BasicAndTags(t *testing.T) {
	v := Outer{
		ID:   42,
		Name: "Alice",
		Inner: Inner{
			Login:    "root",
			Password: "123",
			Hidden:   "secret",
		},
		Numbers: []int{1, 2, 3},
	}
	enc, err := encode(t, v)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Базовые поля
	if got := enc.Fields["ID"]; got != int64(42) && got != 42 { // MapObjectEncoder может хранить как int64
		t.Fatalf("ID = %v, want 42", got)
	}
	if got := enc.Fields["Name"]; got != "Alice" {
		t.Fatalf("Name = %v, want Alice", got)
	}
	// Слайс через AddReflected
	if got := enc.Fields["Numbers"]; !reflect.DeepEqual(got, []int{1, 2, 3}) {
		t.Fatalf("Numbers = %#v, want []int{1,2,3}", got)
	}

	// Вложенная структура
	inner, ok := enc.Fields["Inner"].(map[string]interface{})
	if !ok {
		t.Fatalf("Inner not encoded as nested object, got %#v", enc.Fields["Inner"])
	}
	if inner["Login"] != "root" {
		t.Fatalf("Inner.Login = %v, want root", inner["Login"])
	}
	// Маскировка по тэгам
	if inner["Password"] != "***HIDDEN***" {
		t.Fatalf("Inner.Password = %v, want ***HIDDEN***", inner["Password"])
	}
	if inner["Hidden"] != "***HIDDEN***" {
		t.Fatalf("Inner.Hidden = %v, want ***HIDDEN***", inner["Hidden"])
	}
	// Неэкспортируемые поля пропускаются
	if _, exists := enc.Fields["private"]; exists {
		t.Fatalf("unexpected private field encoded")
	}
}

func TestMarshalSanitizedObject_PointersAndNil(t *testing.T) {
	// nil указатель на входе — ошибок и записей быть не должно
	var p *Outer
	enc := zapcore.NewMapObjectEncoder()
	if err := MarshalSanitizedObject(p, enc); err != nil {
		t.Fatalf("unexpected error for nil pointer: %v", err)
	}
	if len(enc.Fields) != 0 {
		t.Fatalf("want zero fields for nil pointer, got: %#v", enc.Fields)
	}

	// nil в поле указателя сериализуется как строка "nil"
	v := Outer{PInner: nil, PtrNil: nil}
	enc2, err := encode(t, v)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if enc2.Fields["PInner"] != "nil" {
		t.Fatalf("PInner = %v, want \"nil\"", enc2.Fields["PInner"])
	}
	if enc2.Fields["PtrNil"] != "nil" {
		t.Fatalf("PtrNil = %v, want \"nil\"", enc2.Fields["PtrNil"])
	}

	// Невалидный тип на верхнем уровне
	if _, err := encode(t, 123); err == nil {
		t.Fatalf("expected error for non-struct input, got nil")
	}
}

func TestMarshalSanitizedObject_ChannelFieldSkipped(t *testing.T) {
	v := Outer{Ch: make(chan int)}
	enc, err := encode(t, v)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if enc.Fields["Ch"] != "***SKIPPED***" {
		t.Fatalf("Ch = %v, want ***SKIPPED***", enc.Fields["Ch"])
	}
}

func TestMarshalSanitizedObject_MapField_AsNestedObject(t *testing.T) {
	v := Outer{
		M: map[string]interface{}{
			"a": 1,
			"b": "two",
			"c": true,
		},
	}
	enc, err := encode(t, v)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Ожидаем, что поле "M" — вложенный объект с ключами карты
	m, ok := enc.Fields["M"].(map[string]interface{})
	if !ok {
		t.Fatalf("M not encoded as nested object, got %#v", enc.Fields["M"])
	}
	if m["a"] != int64(1) && m["a"] != 1 {
		t.Fatalf("M.a = %v, want 1", m["a"])
	}
	if m["b"] != "two" {
		t.Fatalf("M.b = %v, want two", m["b"])
	}
	if m["c"] != true {
		t.Fatalf("M.c = %v, want true", m["c"])
	}

	// И точно не должно "протекать" в корень
	if _, exists := enc.Fields["a"]; exists {
		t.Fatalf("unexpected top-level key a leaked from map field")
	}
	if _, exists := enc.Fields["b"]; exists {
		t.Fatalf("unexpected top-level key b leaked from map field")
	}
	if _, exists := enc.Fields["c"]; exists {
		t.Fatalf("unexpected top-level key c leaked from map field")
	}
}
