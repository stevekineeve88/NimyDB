package constants

const (
	String   = "string"
	Int      = "int"
	Bool     = "bool"
	DateTime = "datetime"
	Date     = "date"
	Float    = "float"

	KeyMaxLength = 45
	KeyRegex     = "^[a-z_]*$"
	KeyRegexDesc = "snake case"

	BlobMaxLength = 25
	BlobRegex     = "^[a-z_]*$"
	BlobRegexDesc = "snake case"

	BoolValTrue  = "1"
	BoolValFalse = "0"

	MaxPageSize       = 1024 * 10
	MaxIndexSize      = 5024
	IndexPrefixLength = 2

	PagesFile   = "pages.json"
	PagesDir    = "pages"
	FormatFile  = "format.json"
	IndexesFile = "indexes.json"
	IndexesDir  = "indexes"
)

func GetFormatTypes() []string {
	return []string{
		String,
		Int,
		Bool,
		DateTime,
		Date,
		Float,
	}
}

func GetAcceptedBoolValues() []string {
	return []string{
		BoolValTrue,
		BoolValFalse,
	}
}

func GetRecordIdPrefix(recordId string) string {
	return recordId[0:IndexPrefixLength]
}
