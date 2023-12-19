package objects

type Pages interface {
	AddItem(pagesItem PagesItem)
	GetAll() []PagesItem
}

type pages struct {
	pagesItems []PagesItem
}

type PagesItem struct {
	FileName string `json:"fileName"`
}

func CreatePages(pageItems []PagesItem) Pages {
	if pageItems == nil {
		pageItems = make([]PagesItem, 0)
	}
	return pages{
		pagesItems: pageItems,
	}
}

func (p pages) AddItem(pagesItem PagesItem) {
	p.pagesItems = append(p.pagesItems, pagesItem)
}

func (p pages) GetAll() []PagesItem {
	return p.pagesItems
}
