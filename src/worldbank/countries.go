// countries.go module queries  the Worldbank API to retrieve the list of countries for an indicator.
package worldbank

import (
	"log"
	"fmt"
	"strconv"
	"sort"
)

// JSON response format
// [{"page":1,"pages":1,"per_page":"500","total":304}
//  [{"id":"ABW", "iso2Code":"AW", "name":"Aruba",
// 	"region":{ "id":"LCN", "value":"Latin America & Caribbean " },
//  "adminregion":{ "id":"","value":"" },
//  "incomeLevel":{ "id":"HIC", "value":"High income" },
//  "lendingType":{ "id":"LNX", "value":"Not classified" },
//  "capitalCity":"Oranjestad",
//  "longitude":"-70.0167",
//  "latitude":"12.5167"
// }]]

// Worldbank URL fragment for the country list
const (
	WBCountryListURI = "/countries"
)
// Country record from Worldbank.
type WBCountryItem struct {
	Id          string `json:"id"`
	Iso2Code    string `json:"iso2Code"`
	Name        string `json:"name"`
	Region      WBDataPair `json:"region,omitempty"`
	Adminregion WBDataPair `json:"adminregion,omitempty"`
	IncomeLevel WBDataPair `json:"incomeLevel,omitempty"`
	LendingType WBDataPair `json:"lendingType,omitempty"`
	CapitalCity string `json:"capitalcity,omitempty"`
	Longitude   float64 `json:"longitude,omitempty"`
	Latitude    float64 `json:"latitude,omitempty"`
}

// Define types for the sorting.
// By provides the required sorting function "less" method used by sort.Sort.
// The return value determines sort order
type By func(c1, c2 *WBCountryItem) bool

// countrySorter is an utility structure containing the items to sort, and the sort method.
type countrySorter struct {
	countries []WBCountryItem
	by      func(p1, p2 *WBCountryItem) bool // Closure used in the Less method.
}

// This is the method called to sort the data as a method of the By function.
// First assigns the WBCountryItem to countrySorter and the calling "by" type. Second, it calls the sort function.
func (by By) Sort(counryList []WBCountryItem) {
	ps := &countrySorter{countries: counryList, by: by}
	sort.Sort(ps)
}
// Impements the interface for sort.Sort()
func (s *countrySorter) Len() int {
	return len(s.countries)
}
// Impements the interface for sort.Sort()
func (s *countrySorter) Swap(i, j int) {
	s.countries[i], s.countries[j] = s.countries[j], s.countries[i]
}
// Impements the interface for sort.Sort(). This is achieve by calling the function stored in
// countrySorter.by.
func (s *countrySorter) Less(i, j int) bool {
	return s.by(&s.countries[i], &s.countries[j])
}
// A "By" function used to sort by ascending country name.
var nameAscending = func(c1, c2 *WBCountryItem) bool {
	return c1.Name < c2.Name
}
// A "By" function used to sort by ascending ISO 2 code.
var iso2CodeAscending = func(c1, c2 *WBCountryItem) bool {
	return c1.Iso2Code < c2.Iso2Code
}

// Retrieve a list of countries from Worldbank API.
func GetCountryList() []WBCountryItem {
	var itemsPerPage = "500"

	if hc == nil {
		hc = NewHttpClient(WBPopScheme, WBPopHost, WBPopPort, "", "")
	}
	// Fetch the data
	resp, err := hc.getRequest(WBCountryListURI + buildQuery(map[string]string{
		"format":"json", "per_page": itemsPerPage}))
	if err != nil {
		log.Printf("%s\n", err)
		return []WBCountryItem{}
	}

	respMap := decodeResponseToMap(resp).([]interface{})
	// First JSON record is the indicator descriptor
	ri := mapToResponseHdr(respMap[0].(map[string]interface{}))

	// Skip the first record of indicator data
	data := respMap[1].([]interface{})
	var cl []WBCountryItem

	// For each page of results,....
	for pg := 1; pg <= ri.Pages; pg++ {
		log.Printf("CountryList: Load page %d...%d records", pg, len(respMap[1].([]interface{})))
		n := len(data)
		for i := 0; i < n; i++ {
			var cltmp WBCountryItem
			entry := data[i].(map[string]interface{})

			cltmp.Id = entry["id"].(string)
			cltmp.Iso2Code = entry["iso2Code"].(string)
			cltmp.Name = entry["name"].(string)
			if x, ok := entry["region"]; ok {
				cltmp.Region = mapToDataPair(x.(map[string]interface{}))
			}
			if x, ok := entry["adminregion"]; ok {
				cltmp.Adminregion = mapToDataPair(x.(map[string]interface{}))
			}
			if x, ok := entry["incomeLevel"]; ok {
				cltmp.IncomeLevel = mapToDataPair(x.(map[string]interface{}))
			}
			if x, ok := entry["lendingType"]; ok {
				cltmp.LendingType = mapToDataPair(x.(map[string]interface{}))
			}
			if x, ok := entry["capitalCity"]; ok {
				cltmp.CapitalCity = x.(string)
			}
			cltmp.Longitude,_ = strconv.ParseFloat(entry["longitude"].(string), 64)
			cltmp.Latitude, _ = strconv.ParseFloat(entry["latitude"].(string), 64)
			cl = append(cl, cltmp)
		}

		// Fetch next page. Must pass the next page number to Worldbank to get the next page of results.
		resp, err = hc.getRequest(WBCountryListURI + buildQuery(map[string]string{
			"format":"json", "per_page": itemsPerPage, "page": fmt.Sprintf("%d", pg + 1)}))
		if err != nil {
			log.Printf("Partial ist returned. %s\n", err)
			return cl
		}
		respMap = decodeResponseToMap(resp).([]interface{})
		// Skip the first record of indicator data
		respMap = respMap[1].([]interface{})
	}
	// Sort by Name
	By(nameAscending).Sort(cl)
	return cl
}

func SortCountriesById(orig []WBCountryItem){
	By(iso2CodeAscending).Sort(orig)
	return
}