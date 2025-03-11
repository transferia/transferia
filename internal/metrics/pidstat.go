package metrics

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/core/log"
)

type SysInfo struct {
	CPU         float64
	Memory      float64
	Descriptors float64
}

type Stat struct {
	utime  float64
	stime  float64
	cutime float64
	cstime float64
	start  float64
	rss    float64
	uptime float64
}

var (
	platform    string
	history     map[int]Stat
	historyLock sync.Mutex
	eol         string
)

func init() {
	history = make(map[int]Stat)
}

func formatStdOut(stdout []byte, userfulIndex int) []string {
	infoArr := strings.Split(string(stdout), eol)[userfulIndex]
	ret := strings.Fields(infoArr)
	return ret
}

func parseFloat(val string) float64 {
	floatVal, _ := strconv.ParseFloat(val, 64)
	return floatVal
}

func stat(pid int, statType string) (*SysInfo, error) {
	_history := history[pid]
	if statType == "ps" {
		args := "-o pcpu,rss -p"
		if platform == "aix" {
			args = "-o pcpu,rssize -p"
		}
		stdout, _ := exec.Command("ps", args, strconv.Itoa(pid)).Output()
		ret := formatStdOut(stdout, 1)
		if len(ret) == 0 {
			return new(SysInfo), errors.New("Can't find process with this PID: " + strconv.Itoa(pid))
		}
		return &SysInfo{
			CPU:         parseFloat(ret[0]),
			Memory:      parseFloat(ret[1]) * 1024,
			Descriptors: 0,
		}, nil
	} else if statType == "proc" {
		// default clkTck and pageSize
		var clkTck float64 = 100
		var pageSize float64 = 4096

		uptimeFileBytes, _ := os.ReadFile(path.Join("/proc", "uptime"))
		uptime := parseFloat(strings.Split(string(uptimeFileBytes), " ")[0])

		clkTckStdout, err := exec.Command("getconf", "CLK_TCK").Output()
		if err == nil {
			clkTck = parseFloat(formatStdOut(clkTckStdout, 0)[0])
		}

		pageSizeStdout, err := exec.Command("getconf", "PAGESIZE").Output()
		if err == nil {
			pageSize = parseFloat(formatStdOut(pageSizeStdout, 0)[0])
		}

		procStatFileBytes, _ := os.ReadFile(path.Join("/proc", strconv.Itoa(pid), "stat"))
		splitAfter := strings.SplitAfter(string(procStatFileBytes), ")")

		if len(splitAfter) == 0 || len(splitAfter) == 1 {
			return new(SysInfo), errors.New("Can't find process with this PID: " + strconv.Itoa(pid))
		}
		infos := strings.Split(splitAfter[1], " ")
		stat := &Stat{
			utime:  parseFloat(infos[12]),
			stime:  parseFloat(infos[13]),
			cutime: parseFloat(infos[14]),
			cstime: parseFloat(infos[15]),
			start:  parseFloat(infos[20]) / clkTck,
			rss:    parseFloat(infos[22]),
			uptime: uptime,
		}

		_stime := 0.0
		_utime := 0.0
		if _history.stime != 0 {
			_stime = _history.stime
		}

		if _history.utime != 0 {
			_utime = _history.utime
		}
		total := stat.stime - _stime + stat.utime - _utime
		total = total / clkTck

		seconds := stat.start - uptime
		if _history.uptime != 0 {
			seconds = uptime - _history.uptime
		}

		seconds = math.Abs(seconds)
		if seconds == 0 {
			seconds = 1
		}

		historyLock.Lock()
		history[pid] = *stat
		historyLock.Unlock()
		return &SysInfo{
			CPU:         (total / seconds) * 100,
			Memory:      stat.rss * pageSize,
			Descriptors: 0,
		}, nil
	}
	return new(SysInfo), nil
}

func getOpenFilesCount(pid int, platform string) (float64, error) {
	if platform == "win" {
		return 0, nil
	}

	stdout, err := exec.Command("lsof", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, xerrors.Errorf("Can't execute 'lsof -p %v': %w", pid, err)
	}
	return float64(bytes.Count(stdout, []byte(eol)) - 1), nil
}

// GetStat may return incomplete result (with some fields unfilled)
func GetStat(pid int) (*SysInfo, error) {
	platform = runtime.GOOS
	if eol = "\n"; strings.Index(platform, "win") == 0 {
		platform = "win"
		eol = "\r\n"
	}
	statTyp := "ps"
	switch platform {
	case "linux", "netbsd":
		statTyp = "proc"
	case "win":
		statTyp = "win"
	}
	sysInfo, err := stat(pid, statTyp)
	if err != nil {
		return nil, xerrors.Errorf("failed to get stats on platform %q with stats type %q: %w", platform, statTyp, err)
	}
	sysInfo.Descriptors, err = getOpenFilesCount(pid, platform)
	if err != nil {
		logger.Log.Debug(fmt.Sprintf("Failed to get open files count on platform %q", platform), log.Error(err))
	}
	return sysInfo, nil
}
