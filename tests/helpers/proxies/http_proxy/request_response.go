package proxy

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	"github.com/transferia/transferia/pkg/util"
)

type RequestResponse struct {
	ReqMethod string
	ReqURL    string
	ReqBody   []byte
	RespBody  []byte
}

func (r *RequestResponse) Print() {
	currUUID, _ := uuid.NewUUID()
	currUUIDStr := currUUID.String()

	fmt.Printf("PROXY_LOGGED, UUID:%s, ReqMethod:%s, ReqURL:%s\n", currUUIDStr, r.ReqMethod, r.ReqURL)

	if util.IsASCIIPrintable(string(r.ReqBody)) {
		fmt.Printf("PROXY_LOGGED, UUID:%s, REQUEST:%s\n", currUUIDStr, r.ReqBody)
	} else {
		fmt.Printf("PROXY_LOGGED, UUID:%s, HEX(REQUEST):%s\n", currUUIDStr, hex.EncodeToString(r.ReqBody))
	}

	if util.IsASCIIPrintable(string(r.RespBody)) {
		fmt.Printf("PROXY_LOGGED, UUID:%s, RESPONSE:%s\n", currUUIDStr, r.RespBody)
	} else {
		fmt.Printf("PROXY_LOGGED, UUID:%s, HEX(RESPONSE):%s\n", currUUIDStr, hex.EncodeToString(r.RespBody))
	}
}

func isGZIP(header http.Header) bool {
	for k, vv := range header {
		if k == "Content-Encoding" {
			for _, v := range vv {
				if v == "zstd" {
					return true
				}
			}
		}
	}
	return false
}

func unpackZip(in []byte) ([]byte, error) {
	rr, err := zstd.NewReader(bytes.NewReader(in))
	if err != nil {
		return nil, err
	}
	unzippedData, err := io.ReadAll(rr)
	if err != nil {
		return nil, err
	}
	return unzippedData, nil
}

func NewRequestResponse(reqMethod string, reqURL string, srcHeader http.Header, reqBody []byte, respHeader http.Header, respBody []byte) *RequestResponse {
	if isGZIP(srcHeader) {
		reqBodyUnp, err := unpackZip(reqBody)
		if err == nil {
			reqBody = reqBodyUnp
		}
	}
	if isGZIP(respHeader) {
		respBodyUnp, err := unpackZip(respBody)
		if err == nil {
			respBody = respBodyUnp
		}
	}
	return &RequestResponse{
		ReqMethod: reqMethod,
		ReqURL:    reqURL,
		ReqBody:   reqBody,
		RespBody:  respBody,
	}
}

func CheckRequestContains(in []RequestResponse, expectedString string) bool {
	for i := range in {
		if strings.Contains(string(in[i].ReqBody), expectedString) {
			return true
		}
	}
	return false
}
