package conv

import "math"

func BytesToFloat(b []byte) float64 {
	// bytesToFloat converts []byte to float64 without allocating a string
	// Handle empty or nil bytes
	if len(b) == 0 {
		return 0
	}

	var neg bool
	var num, dec uint64
	var decPlaces uint

	i := 0
	// Handle sign
	if b[0] == '-' {
		neg = true
		i++
	} else if b[0] == '+' {
		i++
	}

	// Parse integer part
	for ; i < len(b) && b[i] != '.'; i++ {
		if b[i] >= '0' && b[i] <= '9' {
			num = num*10 + uint64(b[i]-'0')
		}
	}

	// Parse decimal part if exists
	if i < len(b) && b[i] == '.' {
		i++
		for ; i < len(b); i++ {
			if b[i] >= '0' && b[i] <= '9' {
				dec = dec*10 + uint64(b[i]-'0')
				decPlaces++
			}
		}
	}

	// Combine integer and decimal parts
	result := float64(num) + float64(dec)/math.Pow10(int(decPlaces))
	if neg {
		result = -result
	}
	return result

}

func BytesToFloatPtr(b []byte) *float64 {
	f := BytesToFloat(b)
	return &f
}
