package conv

import (
	"math"
	"strconv"
	"testing"
)

func BenchmarkFloatParsing(b *testing.B) {
	testCases := []struct {
		name  string
		input []byte
	}{
		{"Integer", []byte("123")},
		{"Decimal", []byte("123.456")},
		{"Negative", []byte("-123.456")},
		{"LargeNumber", []byte("123456789.123456")},
		{"SmallDecimal", []byte("0.000123456")},
	}

	for _, tc := range testCases {
		b.Run("bytesToFloat/"+tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = BytesToFloat(tc.input)
			}
		})

		b.Run("strconvParseFloat/"+tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = strconv.ParseFloat(string(tc.input), 64)
			}
		})
	}
}

// TestBytesToFloatAccuracy verifies that bytesToFloat produces the same results as strconv.ParseFloat
func TestBytesToFloatAccuracy(t *testing.T) {
	testCases := []struct {
		input string
	}{
		{"123"},
		{"123.456"},
		{"-123.456"},
		{"123456789.123456"},
		{"0.000123456"},
		{"-0.000123456"},
		{"0"},
		{"-0"},
		{"+123.456"},
	}

	for _, tc := range testCases {
		expected, _ := strconv.ParseFloat(tc.input, 64)
		got := BytesToFloat([]byte(tc.input))
		if math.Abs(expected-got) > 1e-10 {
			t.Errorf("For input %s: expected %v, got %v", tc.input, expected, got)
		}
	}
}
