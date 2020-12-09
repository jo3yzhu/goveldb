package utils

// Comparator is used in sorted data structure, in goveldb, it's skip list
// Skip list use IntComparator to determine how to sort kv pair whose key type is int

type Comparator func(lhs, rhs interface{}) int

func IntComparator(lhs, rhs interface{}) int {
	l := lhs.(int)
	r := rhs.(int)

	switch {
	case l > r:
		return 1
	case l < r:
		return -1;
	default:
		return 0
	}
}
