package storage

const (
	OP_SET    = "SET"
	OP_GET    = "GET"
	OP_REMOVE = "REMOVE"
)

const (
	DEFAULT_DIR_PATH = "./minidb"
)

type Command struct {
	Op   string
	Key  string
	Data string
}

type Options struct {
	DirPath string
}

// GetDefaultOptions defines the default settings for database
func GetDefaultOptions() Options {
	return Options{
		DirPath: DEFAULT_DIR_PATH,
	}
}

func CheckOptions(options Options) (Options, error) {
	if options.DirPath == "" {
		options.DirPath = DEFAULT_DIR_PATH
	}

	return options, nil
}
