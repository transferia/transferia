package metrics

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/olekukonko/tablewriter"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/metrics"
	"github.com/transferria/transferria/pkg/format"
	"go.ytsaurus.tech/library/go/core/log"
)

type Ps struct {
	metrics metrics.Registry
}

type RuntimeStat struct {
	memPercentage metrics.Gauge
	memAvailable,
	memUsed,
	cpuCounts,
	runtimeAlloc,
	runtimeTotalAlloc,
	runtimeSys,
	runtimeNumGC,
	runtimeHeapInuse,
	runtimeHeapIdle metrics.Counter
	processCPU         metrics.Gauge
	processRAM         metrics.Gauge
	processDescriptors metrics.Gauge
}

func (p *Ps) Run() {
	r := RuntimeStat{
		memAvailable:       p.metrics.Counter("mem.available"),
		memUsed:            p.metrics.Counter("mem.used"),
		memPercentage:      p.metrics.Gauge("mem.percentage"),
		cpuCounts:          p.metrics.Counter("cpu.counts"),
		runtimeAlloc:       p.metrics.Counter("runtime.alloc"),
		runtimeTotalAlloc:  p.metrics.Counter("runtime.totalAlloc"),
		runtimeSys:         p.metrics.Counter("runtime.sys"),
		runtimeNumGC:       p.metrics.Counter("runtime.numGC"),
		runtimeHeapInuse:   p.metrics.Counter("runtime.heapInuse"),
		runtimeHeapIdle:    p.metrics.Counter("runtime.heapIdle"),
		processCPU:         p.metrics.Gauge("proc.cpu"),
		processRAM:         p.metrics.Gauge("proc.ram"),
		processDescriptors: p.metrics.Gauge("proc.descriptors"),
	}
	var hostCPU []float64
	var processCPU []float64
	var processMEM []float64
	ticker := time.NewTicker(5 * time.Minute)
	for {
		select {
		case <-ticker.C:
			var buf bytes.Buffer
			table := tablewriter.NewWriter(&buf)
			table.SetCaption(true, fmt.Sprintf("Stat for: %v", time.Now()))
			table.SetHeaderLine(true)
			table.SetRowLine(true)
			table.SetHeader([]string{"sensor", "mean", "p50", "p75", "p95", "p99"})
			p.writeCpus(hostCPU, table, "Host CPU")
			p.writeCpus(processCPU, table, "Process CPU")
			memAgr := [5]float64{}
			memAgr[0], _ = stats.Mean(processMEM)
			memAgr[1], _ = stats.Percentile(processMEM, 50)
			memAgr[2], _ = stats.Percentile(processMEM, 75)
			memAgr[3], _ = stats.Percentile(processMEM, 95)
			memAgr[4], _ = stats.Percentile(processMEM, 99)
			memRow := []string{}
			memRow = append(memRow, "MEMORY")
			for _, v := range memAgr {
				memRow = append(memRow, format.SizeInt(int(v)))
			}
			table.Append(memRow)
			table.Render()
			logger.Log.Debugf("Runtime usage:\n%v", buf.String())
			hostCPU = []float64{}
			processCPU = []float64{}
			processMEM = []float64{}
		default:
		}
		time.Sleep(time.Second * 10)
		v, err := mem.VirtualMemory()
		if err != nil {
			logger.Log.Warnf("mem.VirtualMemory returned error: %s", err.Error())
			continue
		}
		r.memAvailable.Add(int64(v.Available))
		r.memUsed.Add(int64(v.Used))
		r.memPercentage.Set(v.UsedPercent)
		c, _ := cpu.Counts(true)
		r.cpuCounts.Add(int64(c))
		var m runtime.MemStats
		st, _ := cpu.Percent(1*time.Second, false)
		hostCPU = append(hostCPU, st...)
		runtime.ReadMemStats(&m)
		r.runtimeAlloc.Add(int64(m.Alloc))
		r.runtimeTotalAlloc.Add(int64(m.TotalAlloc))
		r.runtimeSys.Add(int64(m.Sys))
		r.runtimeNumGC.Add(int64(m.NumGC))
		r.runtimeHeapInuse.Add(int64(m.HeapInuse))
		r.runtimeHeapIdle.Add(int64(m.HeapIdle))

		sysInfo, err := GetStat(os.Getpid())
		if err != nil {
			logger.Log.Warn("Failed to get process' stats", log.Int("pid", os.Getpid()), log.Error(err))
			continue
		}
		processCPU = append(processCPU, sysInfo.CPU)
		processMEM = append(processMEM, sysInfo.Memory)
		r.processCPU.Set(sysInfo.CPU)
		r.processRAM.Set(sysInfo.Memory)
		r.processDescriptors.Set(sysInfo.Descriptors)
	}
}

func (p *Ps) writeCpus(cpuStat []float64, table *tablewriter.Table, sensor string) {
	agr := [5]float64{}
	agr[0], _ = stats.Mean(cpuStat)
	agr[1], _ = stats.Percentile(cpuStat, 50)
	agr[2], _ = stats.Percentile(cpuStat, 75)
	agr[3], _ = stats.Percentile(cpuStat, 95)
	agr[4], _ = stats.Percentile(cpuStat, 99)
	var row []string
	row = append(row, sensor)
	for _, v := range agr {
		row = append(row, fmt.Sprintf("%.2f %%", v))
	}
	table.Append(row)
}

func NewPs(registry metrics.Registry) *Ps {
	psutilRegistry := registry.WithTags(map[string]string{"component": "psutil"})
	p := &Ps{
		metrics: psutilRegistry,
	}

	go p.Run()
	return p
}
