package native

import (
	"fmt"
	"testing"

	"github.com/adrianuswarmenhoven/golog"
	"github.com/adrianuswarmenhoven/golog/term"
)

var called bool

type NoInOut struct{}

func (nio *NoInOut) Test() {
	called = true
}

type InOnly struct {
	A int
	B string
}

func (io *InOnly) Test1(a int) {
	fmt.Printf("calling test1: %d\n", a)
	io.A = a
}

func (io *InOnly) Test2(a int, b string) {
	io.A = a
	io.B = b
}

func TestNoInOut(t *testing.T) {
	nio := &NoInOut{}
	m := Encode(golog.NewMachine(), nio)
	called = false
	res := m.ProveAll(`no_in_out_test(no_in_out).`)
	for _, r := range res {
		if r.Size() > 0 {
			t.Fatalf("Expected no variables, but got: %d in %#v", r.Size(), res)
		}
	}
	if len(res) != 1 {
		t.Fatalf("Expected one solution, but got: %#v", res)
	}
	if !called {
		t.Fatalf("The method wasn't indeed called")
	}
}

func TestInOnly(t *testing.T) {
	io := &InOnly{}
	m := Encode(golog.NewMachine(), io)
	res := m.ProveAll(`X = io([a(A), b(B)]), in_only_test1(X, 42).`)
	for _, r := range res {
		a, b := r.ByName_("A"), r.ByName_("B")
		if !term.IsNumber(a) {
			t.Fatalf("Expected %+v to be a number", a)
		}
		var val int
		if err := NewDecoder(m).DecodeGround(a, &val); err != nil {
			t.Fatalf("Couldn't docode integer: %+v: %s", a, err)
		}
		if val != 42 {
			t.Fatalf("Value was set incorrectly: expected 42, got: %d", val)
		}
		if !term.IsVariable(b) {
			t.Fatalf("Expected %+v to be a variable", b)
		}
	}
	res = m.ProveAll(`X = io([a(A), b(B)]), in_only_test2(X, 42, "foo").`)
	for _, r := range res {
		a, b := r.ByName_("A"), r.ByName_("B")
		if !term.IsNumber(a) {
			t.Fatalf("Expected %+v to be a number", a)
		}
		var val int
		if err := NewDecoder(m).DecodeGround(a, &val); err != nil {
			t.Fatalf("Couldn't docode integer: %+v: %s", a, err)
		}
		if val != 42 {
			t.Fatalf("Value was set incorrectly: expected 42, got: %d", val)
		}
		if !term.IsString(b) {
			t.Fatalf("Expected %+v to be a string", b)
		}
		var sval string
		if err := NewDecoder(m).DecodeGround(b, &sval); err != nil {
			t.Fatalf("Couldn't docode string: %+v: %s", b, err)
		}
		if sval != "foo" {
			t.Fatalf("Value was set incorrectly: expected \"foo\", got: \"%s\"", sval)
		}
	}
}
