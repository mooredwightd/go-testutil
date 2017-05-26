// The testutil package provides an assertion framework for unit testing.
// The package assumes these are executed in the context of gotest, and the first parameter of
// each method is the *testing.T parameter of the test method. If any assertion fails, the assertion
// calls t.Fatalf(), logs the message, and terminates the test method.
//
package gotestutil

import (
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"testing"
	"runtime"
)

// Print a fatal message.
func logFatal(t *testing.T, format string, a []interface{}) {
	//Prepare parameters for CallSlice()
	var args []reflect.Value
	args = append(append(args, reflect.ValueOf(format)), reflect.ValueOf(a))
	// Use reflection to get function Value, and call the variadic function
	pVal := reflect.ValueOf(t.Fatalf)
	pVal.CallSlice(args)
}

// Get the calling function's name. Assists in creating a generic error message.
func assertFuncName(baseOnly bool) string {
	var i = -1
	pc := make([]uintptr, 10)  // at least 1 entry needed
	runtime.Callers(2, pc)
	n := runtime.FuncForPC(pc[0]).Name()
	if baseOnly {
		i = strings.LastIndex(n, ".")
	}
	return n[i + 1: ]
}

// Assert a value is nil.
// The method takes the t paramteter from the test method, the value to be asserted as nil,
// and a message printed if the assertion fails. The format and the a variadic parameters conform
// the the fmt.Fatalf() method.
//
// If the assertion fails, t.Fatalf() is called terminating the test script.
func AssertNil(t *testing.T, v interface{}, format string, a...interface{}) {
	var valid bool = true // False if nil, true of not-nil (valid)
	vVal := reflect.ValueOf(v)
	_ = t

	switch vVal.Kind() {
	default:
		valid = vVal.IsValid()
	case reflect.String:
		valid = vVal.Len() != 0
	case reflect.Chan, reflect.Interface, reflect.Func, reflect.Ptr, reflect.Map, reflect.Slice:
		valid = !vVal.IsNil()
	}

	if valid {
		logFatal(t, assertFuncName(true) + " failed. " + format, a)
	}
}

// Assert a value is NOT nil.
// The method takes the t paramteter from the test method, the value to be asserted as NOT nil,
// and a message printed if the assertion fails. The format and the a variadic parameters conform
// the the fmt.Fatalf() method.
//
// If the assertion fails, t.Fatalf() is called terminating the test script.
func AssertNotNil(t *testing.T, v interface{}, format string, a...interface{}) {
	var valid bool = false
	vVal := reflect.ValueOf(v)

	switch vVal.Kind() {
	default:
		valid = vVal.IsValid()
	case reflect.String:
		valid = vVal.Len() != 0
	case reflect.Interface, reflect.Ptr, reflect.Map, reflect.Slice, reflect.Array:
		valid = !vVal.IsNil()
	}

	if !valid {
		logFatal(t, assertFuncName(true) + " failed. " + format, a)

	}
}

// Assert a value evaluates to true
// The method takes the t parameter  from the test method, the value to be asserted,
// and a message printed if the assertion fails. The format and the a variadic parameters conform
// the the fmt.Fatalf() method.
//
// If the assertion fails, t.Fatalf() is called terminating the test script.
func AssertTrue(t *testing.T, b bool, format string, a...interface{}) {
	if !b {
		logFatal(t, assertFuncName(true) + " failed. " + format, a)
	}
}

// Assert a value evaluates to false
// The method takes the *t parameter  from the test method, the value to be asserted,
// and a message printed if the assertion fails. The format and the a variadic parameters conform
// the the fmt.Fatalf() method.
//
// If the assertion fails, t.Fatalf() is called terminating the test script.
func AssertFalse(t *testing.T, b bool, format string, a...interface{}) {
	if b {
		logFatal(t, assertFuncName(true) + " failed. " + format, a)
	}
}

// Assert a string value is equal to the empty string ("").
// The method takes the t parameter  from the test method, the value to be asserted,
// and a message printed if the assertion fails. The format and the a variadic parameters conform
// the the fmt.Fatalf() method.
//
// If the assertion fails, t.Fatalf() is called terminating the test script.
func AssertEmptyString(t *testing.T, v string, format string, a...interface{}) {
	vVal := reflect.ValueOf(v)
	if vVal.Kind() == reflect.Ptr {
		vVal = vVal.Elem()
	}
	if vVal.Kind() != reflect.String || vVal.Len() != 0 {
		logFatal(t, assertFuncName(true) + " failed. " + format, a)
	}
}

// Assert a string value is NOT equal to the empty string.
// The method takes the *t parameter  from the test method, the value to be asserted,
// and a message printed if the assertion fails. The format and the a variadic parameters conform
// the the fmt.Fatalf() method.
//
// If the assertion fails, t.Fatalf() is called terminating the test script.
func AssertNotEmptyString(t *testing.T, v string, format string, a...interface{}) {
	vVal := reflect.ValueOf(v)
	if vVal.Kind() == reflect.Ptr {
		vVal = vVal.Elem()
	}
	if vVal.Kind() != reflect.String || vVal.Len() == 0 {
		logFatal(t, assertFuncName(true) + " failed. " + format, a)
	}
}

// Assert two strings are equivalent, i.e. have the string characters/runes.
// The method takes the *t parameter  from the test method, the two strings to be asserted,
// and a message printed if the assertion fails. The format and the a variadic parameters conform
// the the fmt.Fatalf() method.
//
// If the assertion fails, t.Fatalf() is called terminating the test script.
func AssertStringsEqual(t *testing.T, v1 string, v2 string, format string, a...interface{}) {
	if v1 != v2 {
		logFatal(t, assertFuncName(true) + " failed. " + format, a)
	}
}

// Assert two strinsg are NOT equivalent, i.e. the string values are not the same.
// The method takes the *t parameter  from the test method, the value to be asserted,
// and a message printed if the assertion fails. The format and the a variadic parameters conform
// the the fmt.Fatalf() method.
//
// If the assertion fails, t.Fatalf() is called terminating the test script.
func AssertStringsNotEqual(t *testing.T, v1 string, v2 string, format string, a...interface{}) {
	if v1 == v2 {
		logFatal(t, assertFuncName(true) + " failed. " + format, a)
	}
}

// Assert that two values are equal.
// The method takes the t parameter  from the test method, the values to be asserted,
// and a message printed if the assertion fails. The format and the a variadic parameters conform
// the the fmt.Fatalf() method. Assumes each of v1 and v2 are typed values (not constants)
//
// If the assertion fails, t.Fatalf() is called terminating the test script.
func AssertEqual(t *testing.T, v1 interface{}, v2 interface{}, format string, a...interface{}) {
	var eq = false

	switch v1.(type) {
	case bool:
		eq = reflect.ValueOf(v1).Bool() == reflect.ValueOf(v2).Bool()
	case int, int8, int16, int32, int64:
		eq = reflect.ValueOf(v1).Int() == reflect.ValueOf(v2).Int()
	case float32, float64:
		eq = reflect.ValueOf(v1).Float() == reflect.ValueOf(v2).Float()
	case complex64, complex128:
		eq = reflect.ValueOf(v1).Complex() == reflect.ValueOf(v2).Complex()
	default:
		eq = reflect.ValueOf(v1).Interface() == reflect.ValueOf(v2).Interface()
	}
	if !eq {
		logFatal(t, assertFuncName(true) + " failed. " + format, a)
	}
}

// Assert the first value (v1) is numerically greater than the second value (v2).
// The method takes the t parameter  from the test method, the values to be asserted,
// and a message printed if the assertion fails. The format and the a variadic parameters conform
// the the fmt.Fatalf() method.
//
// If the assertion fails, t.Fatalf() is called terminating the test script.
func AssertGreaterThan(t *testing.T, v1 interface{}, v2 interface{}, format string, a...interface{}) {
	var gt bool
	switch v1.(type) {
	case int, int8, int16, int32, int64:
		gt = reflect.ValueOf(v1).Int() > reflect.ValueOf(v2).Int()
	case float32, float64:
		gt = reflect.ValueOf(v1).Float() > reflect.ValueOf(v2).Float()
	default:
		gt = false
	}
	if !gt {
		logFatal(t, assertFuncName(true) + " failed. " + format, a)
	}
}

// Assert the first value (v1) is greater-than-or-equal-to the second value (v2)
// The method takes the t parameter  from the test method, the value to be asserted,
// and a message printed if the assertion fails. The format and the a variadic parameters conform
// the the fmt.Fatalf() method.
//
// If the assertion fails, t.Fatalf() is called terminating the test script.
func AssertGreaterThanOrEqual(t *testing.T, v1 interface{}, v2 interface{}, format string, a...interface{}) {
	var gte bool
	switch v1.(type) {
	case int, int8, int16, int32, int64:
		gte = reflect.ValueOf(v1).Int() >= reflect.ValueOf(v2).Int()
	case float32, float64:
		gte = reflect.ValueOf(v1).Float() >= reflect.ValueOf(v2).Float()
	default:
		gte = false
	}
	if !gte {
		logFatal(t, assertFuncName(true) + " failed. " + format, a)
	}
}

// Assert the first value (v1) is less-than the second value (v2)
// The method takes the t parameter  from the test method, the value to be asserted,
// and a message printed if the assertion fails. The format and the a variadic parameters conform
// the the fmt.Fatalf() method.
//
// If the assertion fails, t.Fatalf() is called terminating the test script.
func AssertLessThan(t *testing.T, v1 interface{}, v2 interface{}, format string, a...interface{}) {
	var lt bool
	switch v1.(type) {
	case int, int8, int16, int32, int64:
		lt = reflect.ValueOf(v1).Int() < reflect.ValueOf(v2).Int()
	case float32, float64:
		lt = reflect.ValueOf(v1).Float() < reflect.ValueOf(v2).Float()
	default:
		lt = false
	}
	if !lt {
		logFatal(t, assertFuncName(true) + " failed. " + format, a)
	}
}

// Assert the first value (v1) is less-than-or-equal-to the second value (v2)
// The method takes the *t parameter  from the test method, the value to be asserted,
// and a message printed if the assertion fails. The format and the a variadic parameters conform
// the the fmt.Fatalf() method.
//
// If the assertion fails, t.Fatalf() is called terminating the test script.
func AssertLessThanOrEqual(t *testing.T, v1 interface{}, v2 interface{}, format string, a...interface{}) {
	var lte bool
	switch v1.(type) {
	case int, int8, int16, int32, int64:
		lte = reflect.ValueOf(v1).Int() <= reflect.ValueOf(v2).Int()
	case float32, float64:
		lte = reflect.ValueOf(v1).Float() <= reflect.ValueOf(v2).Float()
	default:
		lte = false
	}
	if !lte {
		logFatal(t, assertFuncName(true) + " failed. " + format, a)
	}
}

// Assert the a function that accepts a parameter (v) returns true.
// The method takes the t parameter  from the test method, the value to be asserted,
// and a message printed if the assertion fails. The format and the a variadic parameters conform
// the the fmt.Fatalf() method.
//
// If the assertion fails, t.Fatalf() is called terminating the test script.
func AssertTrueFunc(t *testing.T, v interface{}, f func(x interface{}) bool, format string, a...interface{}) {
	if !f(v) {
		logFatal(t, assertFuncName(true) + " failed. " + format, a)
	}
}

// Assert the a function that accepts a parameter (v) returns false.
// The method takes the t parameter  from the test method, the value to be asserted,
// and a message printed if the assertion fails. The format and the a variadic parameters conform
// the the fmt.Fatalf() method.
//
// If the assertion fails, t.Fatalf() is called terminating the test script.
func AssertFalseFunc(t *testing.T, v interface{}, f func(x interface{}) bool, format string, a...interface{}) {
	if f(v) {
		logFatal(t, assertFuncName(true) + " failed. " + format, a)
	}
}

// Assert a given string is found in a list of files provided as a map[int]string.
// The method takes the t parameter  from the test method, the value to be asserted,
// and a message printed if the assertion fails. The format and the a variadic parameters conform
// the the fmt.Fatalf() method.
//
// If the assertion fails, t.Fatalf() is called terminating the test script.
func AssertTextInFiles(t *testing.T, fileMap map[int]string, needle string) (found bool) {
	found = scanFiles(fileMap, needle)
	if !found {
		logFatal(t, assertFuncName(true) + " failed. Text: \"%s\" not found in  %v",
			[]interface{}{needle, fileMap})
	}
	return
}

// Assert a given string is NOT found in a list of files provided as a map[int]string.
// The method takes the t parameter  from the test method, the value to be asserted,
// and a message printed if the assertion fails. The format and the a variadic parameters conform
// the the fmt.Fatalf() method.
//
// If the assertion fails, t.Fatalf() is called terminating the test script.
func AssertTextNotInFiles(t *testing.T, fileMap map[int]string, needle string) (found bool) {
	found = scanFiles(fileMap, needle)
	if found {
		logFatal(t, assertFuncName(true) + " failed. Text: \"%s\" not found in  %v",
			[]interface{}{needle, fileMap})
	}
	return
}

// Scan one or more files for a text string.
// Return true if found, otherwise, false.
func scanFiles(fileMap map[int]string, needle string) (found bool) {
	found = true
	for _, v := range fileMap {
		f, oErr := os.Open(v)
		if oErr != nil {
			continue
		}
		buf, _ := ioutil.ReadAll(f)
		isThere := strings.Contains(string(buf), needle)
		found = (found && isThere)
		f.Close()
	}
	return
}
