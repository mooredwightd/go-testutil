// Fetch population survey data from Worldbank.
package worldbank

import (
	"log"
	"fmt"
	"strconv"
	"time"
)

// Worldbank URL for population data
// http://api.worldbank.org/countries/all/indicators/SP.POP.TOTL?date=2007:2017&format=json&page=51
// Response Format:
// [
//     {"page":51,"pages":53,"per_page":"50","total":2640},
//     [{"indicator":{"id":"SP.POP.TOTL","value":"Population, total"},"country":{"id":"UA","value":"Ukraine"},"value":null,"decimal":"0","date":"2016"},
//      {"indicator":{"id":"SP.POP.TOTL","value":"Population, total"},"country":{"id":"UA","value":"Ukraine"},"value":"45154029","decimal":"0","date":"2015"},
//       ....
//      {"indicator":{"id":"SP.POP.TOTL","value":"Population, total"},"country":{"id":"UA","value":"Ukraine"},"value":"45154029","decimal":"0","date":"2015"},
//     ]}
//  }

const (
	WBPopURI = "countries/%s/indicators/SP.POP.TOTL"
)


// Worldbank format for population records
type WBCountryPopDataByYear struct {
	Indicator WBDataPair       `json:"indicator"`
	Country   WBDataPair       `json:"country"`
	Value     string           `json:"value"`
	Decimal   string           `json:"decimal"`
	Date      string           `json:"date"`
}

func getCountryPopURI(id string ) string {
	return fmt.Sprintf(WBPopURI, id)
}

// Type for mapping the JSON into Go data structure.
type CountryPopulationByYear struct {
	Id    string		`json:"id"`
	Name  string		`json:"name"`
	Value uint		`json:"value"`
	Year  string		`json:"year"`
}

// Fetch data for a specific country using the ISO 2 code for the country.
func WBGetPopulationDataById(id string, startYear, endYear string) []CountryPopulationByYear {
	var itemsPerPage = "100"
	var dateRange = ""
	var pd []CountryPopulationByYear

	if hc == nil {
		hc = NewHttpClient(WBPopScheme, WBPopHost, WBPopPort, "", "")
	}

	if startYear == "" {
		startYear = time.Now().Format("YYYY")
	}
	if endYear == "" {
		endYear = startYear
	}
	dateRange = startYear + ":" + endYear

	// Format the URL
	uri := getCountryPopURI(id)
	// Append the query string
	resp, err := hc.getRequest(uri + buildQuery(map[string]string{
		"format":"json", "date": dateRange, "per_page": itemsPerPage}))
	if err != nil {
		log.Printf("%s\n", err)
	}

	respMap := decodeResponseToMap(resp).([]interface{})

	// The first record contains information about the response data.
	// Page number, number of pages, number of records per page, and total number of records.
	ri := mapToResponseHdr(respMap[0].(map[string]interface{}))

	// Skip over the first record to the start of the data
	data := respMap[1].([]interface{})
	log.Printf("Country Population: loading: %d pages, %s items per page %d total items.\n", ri.Pages, ri.PerPage, ri.Total)

	for pg := 1; pg <= ri.Pages; pg++ {
		// Unmarshall from JSON, and append to the slice
		pd = unmarshalPopulationData(data, pd)

		// Fetch next page, must page the page number in the request.
		resp, err := hc.getRequest(uri + buildQuery(map[string]string{
			"format":"json", "per_page": itemsPerPage, "page": fmt.Sprintf("%d", pg + 1)}))
		if err != nil {
			log.Printf("%s\n", err)
		}
		respMap = decodeResponseToMap(resp).([]interface{})
		data = respMap[1].([]interface{})
	}
	//log.Printf("PopulationData: %+v.\n", pd)
	return pd
}

// Unmarshall the Wordbank response for SP_POP_TOTL indicator
func unmarshalPopulationData(data []interface{}, list []CountryPopulationByYear) []CountryPopulationByYear {

	// FOr all records ins the buffer, convert to internal format
	for i := 0; i < len(data); i++ {
		cpdby := CountryPopulationByYear{}
		countryEntry := data[i].(map[string]interface{})
		country  := mapToDataPair(countryEntry["country"].(map[string]interface{}))

		cpdby.Id = country.Id
		cpdby.Name = country.Value
		if x, _ := countryEntry["value"]; x != nil {
			v,_ := strconv.ParseUint(countryEntry["value"].(string), 10, 0)
			cpdby.Value = uint(v)
		}
		if x, _ := countryEntry["date"]; x != nil {
			cpdby.Year = countryEntry["date"].(string)
		}
		list = append(list, cpdby)
	}
	return list
}