package testutil

import (
	"testing"
)

func TestAssertTrue(t *testing.T) {
	AssertTrue(t, true, "Expected true")
}

func TestAssertFalse(t *testing.T) {
	AssertFalse(t, false, "Expected false.")
}

func TestAssertNil(t *testing.T) {
	var b []int
	AssertNil(t, b, "Expected nil.")
}

func TestAssertNotNil(t *testing.T) {
	var b []int = make([]int, 2)
	AssertNotNil(t, b, "Expected nil.")
}

func TestAssertEmptyString(t *testing.T) {
	AssertEmptyString(t, "", "Expected empty string.")
}

func TestAssertNotEmptyString(t *testing.T) {
	AssertNotEmptyString(t, "test", "Expected non-empty string.")
}

func TestAssertStringsEqual(t *testing.T) {
	AssertStringsEqual(t, "test", "test", "Strings did not match.")
}

func TestAssertStringsNotEqualEqual(t *testing.T) {
	AssertStringsNotEqual(t, "test", "test1", "Strings matched.")
}

func TestAssertEqual(t *testing.T) {
	AssertEqual(t, 1, 1, "Integers did not match.")
	AssertEqual(t, 1.0, 1.0, "Floats did not match.")
}

func TestAssertGreaterThan(t *testing.T) {
	AssertGreaterThan(t, 2, 1, "Integer not greater-than.")
	AssertGreaterThan(t, 2.0, 0.0, "Float not greater-than")
}

func TestAssertGreaterThanOrEqual(t *testing.T) {
	AssertGreaterThanOrEqual(t, 2, 1, "Integer not greater-than.")
	AssertGreaterThanOrEqual(t, 2, 2, "Integer not equal.")
	AssertGreaterThanOrEqual(t, 2.0, 1.0, "Float not greater-than.")
	AssertGreaterThanOrEqual(t, 2.0, 2.0, "Float not equal")
}

func TestAssertLessThan(t *testing.T) {
	AssertLessThan(t, -1, 1, "Integer not less-than.")
	AssertLessThan(t, 1, 2, "Integer not less-than.")
	AssertLessThan(t, 1.0, 2.0, "Float not less-than.")
}

func TestAssertLessThanOrEqual(t *testing.T) {
	AssertLessThanOrEqual(t, 1, 2, "Integer not less-than.")
	AssertLessThanOrEqual(t, 2, 2, "Integer not equal.")
	AssertLessThanOrEqual(t, 1.0, 2.0, "Float not less-than.")
	AssertLessThanOrEqual(t, 2.0, 2.0, "Float not equal")
}

func trueFalseTest(v interface{}) bool {
	return v.(bool)
}

func TestAssertTrueFunc(t *testing.T) {
	AssertTrueFunc(t, true, trueFalseTest, "Did not return true.")
}

func TestAssertFalseFunc(t *testing.T) {
	AssertFalseFunc(t, false, trueFalseTest, "Did not return false.")
}
