package internal

import "errors"

var (
	ErrNotFound = errors.New("NotFound")
	ErrDeletion = errors.New("TypeDeletion")
	ErrTableFileMagic = errors.New("ErrTableFileMagic")
	ErrTableTooShort = errors.New("ErrTableTooShort")
)
