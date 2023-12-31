package constants

const (
	String   = "string"
	Int      = "int"
	Bool     = "bool"
	DateTime = "datetime"

	KeyMaxLength = 45
	KeyRegex     = "^[a-z_]*$"
	KeyRegexDesc = "snake case"

	BlobMaxLength = 25
	BlobRegex     = "^[a-z_]*$"
	BlobRegexDesc = "snake case"

	BoolValTrue  = "1"
	BoolValFalse = "0"

	MaxPageSize = 1024 * 10

	PagesFile  = "pages.json"
	FormatFile = "format.json"
)

func GetFormatTypes() []string {
	return []string{
		String,
		Int,
		Bool,
		DateTime,
	}
}

func GetAcceptedBoolValues() []string {
	return []string{
		BoolValTrue,
		BoolValFalse,
	}
}
