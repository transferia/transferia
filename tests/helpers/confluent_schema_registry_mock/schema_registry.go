package confluentsrmock

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type handler func(http.ResponseWriter, *http.Request)

type ConfluentSRMock struct {
	mutex         sync.Mutex
	callback      handler
	server        *httptest.Server
	uriToCallback map[endpointMatcher]handler

	subjectToLocalDataToVer map[string]map[string]int
	subjectToVerToLocalData map[string]map[int]string
	globalVerToData         map[int]string
}

func (m *ConfluentSRMock) Close() {
	m.server.Close()
}

func (m *ConfluentSRMock) URL() string {
	return m.server.URL
}

func (m *ConfluentSRMock) schemasIDS(w http.ResponseWriter, r *http.Request) {
	idStr := strings.TrimPrefix(r.URL.Path, "/schemas/ids/")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		fmt.Printf("unable to extract ids from string, str: %s\n", idStr)
		return
	}
	ans, ok := m.globalVerToData[id]
	if ok {
		_, _ = w.Write([]byte(ans))
	} else {
		http.NotFound(w, r)
	}
}

func (m *ConfluentSRMock) add(subject, data string) int {
	if _, ok := m.subjectToLocalDataToVer[subject]; !ok {
		m.subjectToLocalDataToVer[subject] = make(map[string]int)
		m.subjectToVerToLocalData[subject] = make(map[int]string)
	}
	newLocalNum := len(m.subjectToLocalDataToVer[subject]) + 1
	m.subjectToLocalDataToVer[subject][data] = newLocalNum
	m.subjectToVerToLocalData[subject][newLocalNum] = data
	newGlobalNum := len(m.globalVerToData) + 1
	m.globalVerToData[newGlobalNum] = data
	return newGlobalNum
}

func (m *ConfluentSRMock) Versions(w http.ResponseWriter, r *http.Request) {
	url := r.URL.Path
	subject := url[len("/subjects/") : len(url)-len("/versions")]
	defer func() { _ = r.Body.Close() }()
	buf, err := io.ReadAll(r.Body)
	if err != nil {
		fmt.Printf("unable to read post data, err: %s\n", err)
		return
	}
	postData := string(buf)
	result, ok := m.subjectToLocalDataToVer[subject][postData]
	if ok {
		_, _ = w.Write([]byte(fmt.Sprintf(`{"id":%d}`, result)))
	} else {
		newGlobalNum := m.add(subject, postData)
		_, _ = w.Write([]byte(fmt.Sprintf(`{"id":%d}`, newGlobalNum)))
	}
}

func (m *ConfluentSRMock) Schema(w http.ResponseWriter, r *http.Request) {
	url := r.URL.Path
	match := reSchema.FindStringSubmatch(url)
	if len(match) != 3 {
		fmt.Printf("unable to match url:%s\n", url)
		return
	}
	subject := match[1]
	verStr := match[2]
	ver, _ := strconv.Atoi(verStr)

	body := m.subjectToVerToLocalData[subject][ver]
	_, _ = w.Write([]byte(body))
}

func (m *ConfluentSRMock) AddSchema(t *testing.T, subject string, subjectVersion, globalVersion int, data string) {
	newGlobalNum := m.add(subject, data)

	require.Equal(t, globalVersion, newGlobalNum)
	require.Equal(t, subjectVersion, len(m.subjectToLocalDataToVer[subject]))
}

func NewConfluentSRMock(globalIDToData map[int]string, callback handler) *ConfluentSRMock {
	if globalIDToData == nil {
		globalIDToData = make(map[int]string)
	}
	mock := &ConfluentSRMock{
		mutex:                   sync.Mutex{},
		callback:                callback,
		server:                  nil,
		subjectToLocalDataToVer: make(map[string]map[string]int),
		subjectToVerToLocalData: make(map[string]map[int]string),
		globalVerToData:         globalIDToData,
		uriToCallback:           make(map[endpointMatcher]handler),
	}
	mock.server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mock.mutex.Lock()
		defer mock.mutex.Unlock()
		if mock.callback != nil {
			mock.callback(w, r)
		}
		for currMatcher, currHandler := range mock.uriToCallback {
			if currMatcher.IsMatched(r.URL.Path) {
				currHandler(w, r)
			}
		}
	}))

	addEndpoint := func(matcher endpointMatcher, inHandler handler) {
		mock.uriToCallback[matcher] = inHandler
	}

	addEndpoint(schemasIDS{}, mock.schemasIDS)
	addEndpoint(subjectsVersions{}, mock.Versions)
	addEndpoint(schema{}, mock.Schema)

	mock.server.Start()

	return mock
}
