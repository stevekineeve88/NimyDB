package objects

type Format interface {
	AddItem(key string, mapItem FormatItem) bool
	GetMap() map[string]FormatItem
}

type format struct {
	formatMap map[string]FormatItem
}

type FormatItem struct {
	KeyType string `json:"keyType"`
}

func CreateFormat(formatMap map[string]FormatItem) Format {
	if formatMap == nil {
		formatMap = make(map[string]FormatItem)
	}
	return format{
		formatMap: formatMap,
	}
}

func (f format) AddItem(key string, formatItem FormatItem) bool {
	if _, ok := f.formatMap[key]; ok {
		return false
	}
	f.formatMap[key] = formatItem
	return true
}

func (f format) GetMap() map[string]FormatItem {
	return f.formatMap
}
