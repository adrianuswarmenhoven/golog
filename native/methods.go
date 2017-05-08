package native

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/mndrix/golog"
	"github.com/mndrix/golog/term"
)

func Encode(m golog.Machine, obj interface{}) golog.Machine {
	return m.RegisterForeign(GenerateMethods(obj))
}

func canElement(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Array, reflect.Chan, reflect.Map, reflect.Ptr, reflect.Slice:
		return true
	}
	return false
}

func GenerateAccessors(obj interface{}) map[string]golog.ForeignPredicate {
	t := reflect.TypeOf(obj)
	result := map[string]golog.ForeignPredicate{}
	for i := 0; i < t.NumField(); i++ {
		name := t.Name()
		if name == "" && canElement(t) {
			name = t.Elem().Name()
		}
		k, v := GenerateAccessor(name, t, t.Field(i))
		result[k] = v
	}
	return result
}

func GenerateAccessor(name string, t reflect.Type, f reflect.StructField) (string, golog.ForeignPredicate) {
	fc := &ForeignCall{
		Signature: Signature([]Argument{
			newAnyArg(nil, t),
			newAnyArg(nil, f.Type),
		}),
		Name:  gpName(fmt.Sprintf("%s_%s", name, f.Name)),
		Arity: 2,
		Type:  t,
		Field: f,
	}
	return fc.String(), fc.Accessor
}

func GenerateMethods(obj interface{}) map[string]golog.ForeignPredicate {
	t := reflect.TypeOf(obj)
	result := map[string]golog.ForeignPredicate{}
	for i := 0; i < t.NumMethod(); i++ {
		name := t.Name()
		if name == "" && canElement(t) {
			name = t.Elem().Name()
		}
		k, v := GenerateMethod(name, t.Method(i))
		result[k] = v
	}
	return result
}

func GenerateMethod(name string, m reflect.Method) (string, golog.ForeignPredicate) {
	fc := &ForeignCall{
		Signature: NewSignature(m),
		Name:      gpName(fmt.Sprintf("%s_%s", name, m.Name)),
		Arity:     uint(m.Type.NumIn() + m.Type.NumOut()),
		Method:    m,
	}
	return fc.String(), fc.Predicate
}

type Argument interface {
	fmt.Stringer
	IsValid(golog.Machine, term.Term) bool
}

type typedArg struct {
	pType *int
	gType reflect.Type
}

func (ta *typedArg) isValid(m golog.Machine, t term.Term) bool {
	if ta.pType != nil && *ta.pType != t.Type() {
		return false
	}
	if ta.gType != nil {
		if IsNative(t) {
			gt := reflect.TypeOf(t.(*Native).val)
			if !gt.AssignableTo(ta.gType) {
				return false
			}
		} else {
			d := NewDecoder(m)
			p := reflect.New(ta.gType)
			if err := d.DecodeGround(t, p.Interface()); err != nil {
				return false
			}
		}
	}
	return true
}

func (ta *typedArg) String() string {
	mode := "?"
	if ta.pType != nil {
		switch *ta.pType {
		case term.VariableType:
			mode = "-"
		case term.FloatType, term.IntegerType, term.AtomType:
			mode = "+"
		}
	}
	name := "%s"
	if ta.gType != nil {
		t := strings.Replace(ta.gType.String(), ".", "_", -1)
		t = strings.Replace(t, "*", "ptr_", -1)
		name += "_" + t
	}
	return mode + name
}

type anyArg struct {
	*typedArg
}

func newAnyArg(pType *int, gType reflect.Type) *anyArg {
	return &anyArg{
		typedArg: &typedArg{
			pType: pType,
			gType: gType,
		},
	}
}

func (ga *anyArg) IsValid(m golog.Machine, t term.Term) bool {
	if term.IsVariable(t) {
		return true
	}
	return ga.isValid(m, t)
}

type varArg struct{}

func (va *varArg) String() string {
	return "-%s"
}

func newVarArg(pType *int, gType reflect.Type) *varArg {
	return &varArg{}
}

func (va *varArg) IsValid(m golog.Machine, t term.Term) bool {
	return term.IsVariable(t)
}

type groundArg struct {
	*typedArg
}

func newGroundArg(pType *int, gType reflect.Type) *groundArg {
	return &groundArg{
		typedArg: &typedArg{
			pType: pType,
			gType: gType,
		},
	}
}

func (ga *groundArg) String() string {
	return "+" + ga.typedArg.String()[1:]
}

func (ga *groundArg) IsValid(m golog.Machine, t term.Term) bool {
	if !ga.isGround(t) {
		return false
	}
	return ga.isValid(m, t)
}

func (ga *groundArg) isGround(t term.Term) bool {
	switch t.Type() {
	case term.VariableType:
		return false
	case term.AtomType,
		term.IntegerType,
		term.FloatType,
		term.ErrorType:
		return true
	case term.CompoundType:
		x := t.(*term.Compound)
		for _, arg := range x.Arguments() {
			if !ga.isGround(arg) {
				return false
			}
		}
		return true
	}
	panic(fmt.Sprintf("Unexpected term type: %#v", t))
}

type Signature []Argument

func NewSignature(m reflect.Method) Signature {
	var args []Argument
	for i := 0; i < m.Type.NumIn(); i++ {
		// TODO(wvxvw): Do something about Prolog types, eg. if we
		// know that the argument type is numerical, make sure
		// groundArg will have its Prolog type reflect that.
		args = append(args, newAnyArg(nil, m.Type.In(i)))
	}
	for i := 0; i < m.Type.NumOut(); i++ {
		args = append(args, newAnyArg(nil, m.Type.In(i)))
	}
	return Signature(args)
}

func (s Signature) IsValid(m golog.Machine, args []term.Term) bool {
	if len(s) != len(args) {
		return false
	}
	for i, a := range s {
		if !a.IsValid(m, args[i]) {
			return false
		}
	}
	return true
}

func (s Signature) Args(
	method reflect.Method, m golog.Machine, args []term.Term,
) (result []reflect.Value, watchers []*Watcher, err error) {
	d := NewDecoder(m)
	for i := 0; i < method.Type.NumIn(); i++ {
		t := method.Type.In(i)
		val := reflect.New(t)
		if canElement(t) {
			// TODO(wvxvw): Handle arbitrary long chain of pointers to
			// pointers to pointers...
			val.Elem().Set(reflect.New(t.Elem()))
		}
		dval := val.Interface()
		ws, err := d.Decode(args[i], dval)
		if err != nil {
			return nil, nil, err
		}
		result = append(result, val.Elem())
		watchers = append(watchers, ws...)
	}
	return result, watchers, nil
}

func (s Signature) String() string {
	var chunks []string
	name := 'A'
	for _, sc := range s {
		chunks = append(chunks, fmt.Sprintf(sc.String(), string([]rune{name})))
		name++
	}
	return strings.Join(chunks, " -> ")
}

type ForeignCall struct {
	Signature Signature
	Name      string
	Arity     uint
	Method    reflect.Method
	Type      reflect.Type
	Field     reflect.StructField
}

func (fc *ForeignCall) String() string {
	return fmt.Sprintf("%s/%d", fc.Name, fc.Arity)
}

func (fc *ForeignCall) Predicate(m golog.Machine, args []term.Term) golog.ForeignReturn {
	if !fc.Signature.IsValid(m, args) {
		return golog.ForeignFail()
	}
	fargs, watchers, err := fc.Signature.Args(fc.Method, m, args)
	if err != nil {
		return golog.ForeignFail()
	}
	res := fc.Method.Func.Call(fargs)
	ures := args[len(args)-len(res):]
	e := NewEncoder()
	var unified []term.Term
	for i, r := range res {
		t := e.Encode(r.Interface())
		unified = append(unified, ures[i], t)
	}
	for _, w := range watchers {
		// TODO(wvxvw): Handle cases when the same varialbe occures in
		// two different arguments.  Both arguments may have been
		// modified by the wrapped method, thus it is possible that if
		// the variable appears more than once these will happend:
		//
		// 1. Variable was not instantiated even once.
		//
		// 2. Variable was instantiated to the same value multiple
		//    times.
		//
		// 3. Variable was instantiated to different values.
		//
		// 4. Variable was instantiated fewer times than it occurs in
		//    arguments.
		//
		// In case (3) the predicate should fail (unfortunately, the
		// sideffect cannot be undone).  In case (4) we need to set
		// the value watched by the watchers assigned to
		// uninstantiated variables.
		if w.HasChanged() {
			unified = append(unified, w.Variable, e.Encode(w.Value()))
		}
	}
	return golog.ForeignUnify(unified...)
}

func (fc *ForeignCall) Accessor(m golog.Machine, args []term.Term) golog.ForeignReturn {
	if !fc.Signature.IsValid(m, args) {
		return golog.ForeignFail()
	}
	d := NewDecoder(m)
	val := reflect.New(fc.Type).Interface()
	watchers, err := d.Decode(args[0], val)
	if err != nil {
		return golog.ForeignFail()
	}
	lval := reflect.ValueOf(val)
	field := lval.Field(fc.Field.Index)
	rval := reflect.New(fc.Field.Type).Interface()
	rwatchers, err := d.Decode(args[1], rval)
	if err != nil {
		return golog.ForeignFail()
	}
	field.Set(reflect.ValueOf(rval).Elem())
	var unified []term.Term
	return golog.ForeignUnify(unified...)
}
