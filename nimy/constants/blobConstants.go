package constants

const (
	String       = "string"
	Int32        = "int32"
	Int64        = "int64"
	Bool         = "bool"
	DateTime     = "datetime"
	KeyMaxLength = 45
	KeyRegex     = "^[a-z_]*$"
	KeyRegexDesc = "snake case"

	BlobMaxLength = 25
	BlobRegex     = "^[a-z_]*$"
	BlobRegexDesc = "snake case"
)

func GetFormatTypes() []string {
	return []string{
		String,
		Int32,
		Int64,
		Bool,
		DateTime,
	}
}
