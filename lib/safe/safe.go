package safe

func Bool(v *bool) bool {
	if v == nil {
		return false
	}
	return *v
}
