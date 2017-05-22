package worldbank

// Worldbank response header format
type WBResponseHdr struct {
	Page    int     `json:"page"`
	Pages   int     `json:"pages"`
	PerPage string  `json:"per_page"`
	Total   uint64  `json:"total"`
}

// Worldbank format for "indicators"
type WBDataPair struct {
	Id    string        `json:"id"`
	Value string        `json:"value"`
}

const (
	WBPopScheme = "http"
	WBPopHost = "api.worldbank.org"
	WBPopPort = 80
)

var (
	hc *HttpConnection
)

func init() {

}

// Map a API response header to WBResponseHdr
// m is a map for JSON fields "page", "per_page", "pages", and "total".
func mapToResponseHdr(m map[string]interface{}) WBResponseHdr {
	ri := WBResponseHdr{
		Page: int(m["page"].(float64)),
		PerPage: m["per_page"].(string),
		Pages: int(m["pages"].(float64)),
		Total: uint64(m["total"].(float64)),
	}
	return ri
}

// m is a map with values for JSON fields "id" and "value"
func mapToDataPair(m map[string]interface{}) WBDataPair {
	wbdp := WBDataPair{}
	if i, ok := m["id"].(string); ok {
		wbdp.Id = i
	}
	if i, ok := m["value"].(string); ok {
		wbdp.Value = i
	}
	return wbdp
}