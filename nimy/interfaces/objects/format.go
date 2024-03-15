package objects

type Format struct {
	FormatMap map[string]FormatItem `json:"format_map,required"`
}

type FormatItem struct {
	KeyType string `json:"keyType"`
}

func CreateFormat(formatMap map[string]FormatItem) Format {
	if formatMap == nil {
		formatMap = make(map[string]FormatItem)
	}
	return Format{
		FormatMap: formatMap,
	}
}

func (f *Format) AddItem(key string, formatItem FormatItem) bool {
	if _, ok := f.FormatMap[key]; ok {
		return false
	}
	f.FormatMap[key] = formatItem
	return true
}

func (f *Format) GetMap() map[string]FormatItem {
	return f.FormatMap
}
