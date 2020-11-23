package common

import "errors"

// Errors types used.
var (
	ErrNotFound       = errors.New("key not found")
	ErrNotInitialized = errors.New("Tree is not initialized")
	ErrInvalidParam   = errors.New("Invalid configuration parameter")
	ErrExists         = errors.New("Already exists")
	ErrTooLarge       = errors.New("Too many values")
	ErrDBLoadFailed   = errors.New("Failed to load from DB")
	ErrDBUpdateFailed = errors.New("Failed to update DB")
	ErrFull           = errors.New("cache is full, something got evicted")
)
