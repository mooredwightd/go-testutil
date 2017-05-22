package worldbank

import (
	"testing"
	"github.com/go-testutil/testutil"
	"flag"
	"strconv"
	"regexp"
)

var (
	doDataLoad = false
	verbose = false
)

func init() {
	flag.BoolVar(&doDataLoad, "loaddata", false, "Load data from WorldBank")
	flag.BoolVar(&verbose, "verbose", false, "Display verbose messages.")
	flag.Parse()
}

func TestGetCountryList(t *testing.T) {
	cList := GetCountryList()
	testutil.AssertGreaterThan(t, len(cList), 0, "Expected result count > 0.")
	if verbose {
		for i, v := range cList {
			t.Logf("%d) Country \"%s\" (%s, %s, Region: %s)\n", i, v.Name, v.Id, v.Iso2Code, v.Region.Value)
		}
	}
}

func TestGetPopulationDataById(t *testing.T) {
	pList := WBGetPopulationDataById("US", "2007", "2017")
	testutil.AssertGreaterThan(t, len(pList), 0, "Expected result count > 0.")
	if verbose {
		for i, v := range pList {
			t.Logf("%d) %s (%s) %s for %s\n", i, v.Name, v.Id, formatUint(v.Value), v.Year)
		}
	}
}

var re = regexp.MustCompile("(\\d+)(\\d{3})")

func formatUint(u uint) string {
	str := strconv.FormatUint(uint64(u), 10)
	for i := 0; i < (len(str) - 1) / 3; i++ {
		str = re.ReplaceAllString(str, "$1,$2")
	}
	return str
}
