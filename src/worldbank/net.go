// Utility functions to fetch data from Worldbank API
package worldbank

import (
	"net/http"
	"encoding/json"
	"fmt"
	"bytes"
	"io/ioutil"
	"log"
)

// Error informatino
type NetErrorInfo struct {
	Msg string
	Err error
}

func (e NetErrorInfo) Error() string {
	return fmt.Sprintf("net: %s. %s", e.Msg, e.Err.Error())
}

func NetError(errType, err string, data map[string]string) (error) {
	ne := NetErrorInfo{
		Msg: fmt.Sprintf("net. %s %s error. %v.", errType, err, data),
	}
	return ne
}
func netUrlError(module string, err string, data map[string]string) (error) {
	_ = module
	return NetError("URL: " + module, err, data)
}
func netRequestError(module string, err string, data map[string]string) (error) {
	return NetError("REQUEST: " + module, err, data)
}
func netResponseError(module string, err string, data map[string]string) (error) {
	return NetError("RESPONSE: " + module, err, data)
}
func netJsonError(module string, err string, data map[string]string) (error) {
	return NetError("JSON: " + module, err, data)
}


// HTTP Utility function for initiating requests
// Create a new repository context
// 	host is the host server name, e.g. www.google.com
// 	port is the port number for the connection
// 	username/password are the credentials for the connection
type HttpConnection struct {
	username string            `json:"-"`
	password string            `json:"-"`
	scheme   string            `json:"scheme"`
	host     string            `json:"host"`
	port     int               `json:"port"`
	jwt      string            `json:"-"`
	client   *http.Client      `json:"-"`
}

// Build a url
func (hc *HttpConnection) buildUrl(path string) string {

	if path[0] != '/' {
		path = string("/" + path)
	}
	url := fmt.Sprintf("%s://%s:%d%s", hc.scheme, hc.host, hc.port,path)
	return url
}

func NewHttpClient(scheme, host string, port int, username string, password string) *HttpConnection {
	hc := &HttpConnection{
		scheme: scheme,
		host:     host,
		port:     port,
		client: &http.Client{},
		username: username,
		password: password}
	defer func() {
		if x := recover(); x != nil {
			hc = nil
		}
	}()
	return hc
}

// Utility function that allows for an array to set HTTP header options
func newRequest(method, url string, data []byte, hdrOpts map[string]string) (*http.Request, error) {
	var req *http.Request
	var err error
	_ = hdrOpts
	if len(data) == 0 {
		req, err = http.NewRequest(method, url, nil)

	} else {
		req, err = http.NewRequest(method, url, bytes.NewBuffer(data))
	}
	if err != nil {
		ne := netUrlError("newRequest()", err.Error(), map[string]string{"method": method, "url": url})
		log.Println(ne)
		return nil, ne
	}

	return req, nil
}

func (hc *HttpConnection) doRequestFullRead(method string, url string, jsonStr []byte) (bodyBuf []byte, err error) {
	var hdr = map[string]string{"Content-Type": "application/json"}

	// Create  request
	req, err := newRequest(method, hc.buildUrl(url), jsonStr, hdr)
	if err != nil {
		return nil, err
	}

	// Issue the request
	resp, err := hc.client.Do(req)
	if err != nil {
		return nil, netResponseError(method, err.Error(), map[string]string{"url": url})
	}

	// Read the response
	defer resp.Body.Close()
	bodyBuf, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, netResponseError(method, err.Error(),
			map[string]string{"url": url, "body": string(bodyBuf)})
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return nil, netResponseError(method, "Read error",
			map[string]string{"url": url, "response": string(bodyBuf)})
	}

	return bodyBuf, nil
}

func (hc *HttpConnection) getRequest(url string) ([]byte, error) {
	return hc.doRequestFullRead(http.MethodGet, url, nil)
}

// Decode JSON string into a map[string]interface{}
// A general method for unmarshalling a response string.
func decodeResponseToMap(jsonString []byte) interface{} {
	var respBuf interface{}

	err := json.Unmarshal(jsonString, &respBuf)
	if err != nil {
		panic(netJsonError("DECODE", err.Error(),
			map[string]string{"body": string(jsonString)}))
	}
	return respBuf.(interface{})
}

// Build a query string from a map[string]string where
//    the map key is the parameter name, and the map value is the parameter value
func buildQuery(qParams map[string]string) (qString string) {

	if len(qParams) == 0 {
		return ""
	}

	qString = "?"
	for i, p := range qParams {
		qString = qString + i + "=" + p + "&"
	}
	qString = qString[0 : len(qString)-1]
	return
}