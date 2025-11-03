package proxy

import (
	"fmt"
	"testing"
	"time"
)

func TestHTTPProxy(t *testing.T) {
	t.Skip()

	proxy := NewHTTPProxy(4321, "localhost:80")
	worker := proxy.RunAsync()
	defer worker.Close()

	for {
		if proxy.Err != nil {
			panic(proxy.Err)
		}
		if len(proxy.GetSniffedData()) >= 3 {
			break
		}
		time.Sleep(time.Second)
		fmt.Println("TEST", time.Now())
		fmt.Println(len(proxy.GetSniffedData()))
	}
}

func TestHTTPProxyWithPortAllocation(t *testing.T) {
	t.Skip()

	proxy := NewHTTPProxy(0, "localhost:80")
	worker := proxy.RunAsync()
	defer worker.Close()

	fmt.Println("Allocated port", proxy.ListenPort)

	for {
		if proxy.Err != nil {
			panic(proxy.Err)
		}
		if len(proxy.GetSniffedData()) >= 3 {
			break
		}
		time.Sleep(time.Second)
		fmt.Println("TEST", time.Now())
		fmt.Println(len(proxy.GetSniffedData()))
	}
}
