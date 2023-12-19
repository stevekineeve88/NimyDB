package objects

type Format interface {
	AddItem(col string, mapItem FormatItem) bool
	GetMap() map[string]FormatItem
}

type format struct {
	formatMap map[string]FormatItem
}

type FormatItem struct {
	ColType string `json:"colType"`
}

func CreateFormat(formatMap map[string]FormatItem) Format {
	if formatMap == nil {
		formatMap = make(map[string]FormatItem)
	}
	return format{
		formatMap: formatMap,
	}
}

func (f format) AddItem(col string, formatItem FormatItem) bool {
	if _, ok := f.formatMap[col]; ok {
		return false
	}
	f.formatMap[col] = formatItem
	return true
}

func (f format) GetMap() map[string]FormatItem {
	return f.formatMap
}
