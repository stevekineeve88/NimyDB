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

	MaxPageSize       = 1024 * 50
	MaxIndexSize      = 5024 * 100
	IndexPrefixLength = 2

	PagesFile   = "pages.json"
	PagesDir    = "pages"
	FormatFile  = "format.json"
	IndexesFile = "indexes.json"
	IndexesDir  = "indexes"

	SearchThreadCount = 5
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

func GetRecordIdPrefix(recordId string) string {
	return recordId[0:IndexPrefixLength]
}
