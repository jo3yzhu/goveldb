package internal

const (
	MaxOpenFiles           = 1000
	NumNonTableCacheFiles  = 10
	NumLevels              = 7
	MaxMemCompactLevel     = 2
	L0CompactionTrigger    = 4
	MaxFileSize            = 2 << 20
	L0SlowdownWriteTrigger = 8
	WriteBufferSize        = 4 << 20
)
