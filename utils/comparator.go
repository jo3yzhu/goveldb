package utils

// Comparator is used in sorted data structure, in goveldb, it's skip list
// Skip list use IntComparator to determine how to sort kv pair whose key type is int

type Comparator func(lhs, rhs interface{}) int

// @description:
// @return: +1 if a > b, -1 if a < b, 0 if a == b

func IntComparator(a, b interface{}) int {
	l := a.(int)
	r := b.(int)

	switch {
	case l > r:
		return +1
	case l < r:
		return -1;
	default:
		return 0
	}
}
