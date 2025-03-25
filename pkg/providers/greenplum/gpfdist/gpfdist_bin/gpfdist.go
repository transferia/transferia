package gpfdistbin

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/core/xerrors/multierr"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/terryid"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	openFifoTimeout = 600 * time.Second
	defaultPipeMode = uint32(0644)
	minPort         = 8500
	maxPort         = 8600
)

type GpfdistMode string

const (
	ExportTable = GpfdistMode("export-table")
	ImportTable = GpfdistMode("import-table")
)

type Gpfdist struct {
	cmd           *exec.Cmd // cmd is a command to run gpfdist executable.
	host          string
	port          int
	workingDir    string
	serviceSchema string
	pipeName      string
	ddlExecutor   *GpfdistDDLExecutor
	mode          GpfdistMode
}

func (g *Gpfdist) Stop() error {
	var errors []error
	if err := g.removePipe(); err != nil {
		errors = append(errors, xerrors.Errorf("unable to remove pipe: %w", err))
	}
	if g.cmd.Process != nil {
		if err := g.cmd.Process.Kill(); err != nil {
			errors = append(errors, xerrors.Errorf("unable to kill process: %w", err))
		}
	} else {
		logger.Log.Warnf("Gpfdist process is nil, won't be killed")
	}
	return multierr.Combine(errors...)
}

func (g *Gpfdist) RunExternalTableTransaction(ctx context.Context, table abstract.TableID, schema *abstract.TableSchema) (int64, error) {
	return g.ddlExecutor.createExternalTableAndInsertRows(ctx, g.externalTableMode(), table, schema, g.serviceSchema, g.locations())
}

func (g *Gpfdist) externalTableMode() externalTableMode {
	if g.mode == ExportTable {
		return modeWritable
	}
	return modeReadable
}

func (g *Gpfdist) pipeOpenFlag() int {
	if g.mode == ExportTable {
		return os.O_RDONLY
	}
	return os.O_WRONLY
}

func (g *Gpfdist) OpenPipe() (*os.File, error) {
	var cancelFlag int
	switch g.pipeOpenFlag() {
	case os.O_RDONLY:
		cancelFlag = os.O_WRONLY | syscall.O_NONBLOCK
	case os.O_WRONLY:
		cancelFlag = os.O_RDONLY | syscall.O_NONBLOCK
	}

	pipePath := g.fullPath(g.pipeName)
	var file *os.File
	openFile := func() error {
		var openErr error
		file, openErr = os.OpenFile(pipePath, g.pipeOpenFlag(), 0)
		return openErr
	}
	cancelOpenFile := func() error {
		file, openErr := os.OpenFile(pipePath, cancelFlag, 0)
		if openErr != nil {
			return xerrors.Errorf("unable to open cancellation file %s with flag '%d': %w", pipePath, cancelFlag, openErr)
		}
		return file.Close()
	}

	if err := tryFunction(openFile, cancelOpenFile, openFifoTimeout); err != nil {
		if xerrors.As(err, new(CancelFailedError)) {
			err = abstract.NewFatalError(err)
		}
		return nil, xerrors.Errorf("unable to open pipe %s file: %w", g.pipeName, err)
	}
	return file, nil
}

// fullPath concatenates working directory and "/" to the left of provided relative path.
func (g *Gpfdist) fullPath(relativePath string) string {
	return fmt.Sprintf("%s/%s", g.workingDir, relativePath)
}

func (g *Gpfdist) locations() []string {
	return []string{fmt.Sprintf("gpfdist://%s:%d/%s", g.host, g.port, g.pipeName)}
}

func (g *Gpfdist) removePipe() error {
	logger.Log.Infof("Removing pipe %s", g.pipeName)
	return os.Remove(g.fullPath(g.pipeName))
}

func (g *Gpfdist) initPipe() error {
	logger.Log.Infof("Creating pipe %s", g.pipeName)
	return syscall.Mkfifo(g.fullPath(g.pipeName), defaultPipeMode)
}

func InitGpfdist(params GpfdistParams, mode GpfdistMode, conn *pgxpool.Pool) (*Gpfdist, error) {
	switch mode {
	case ExportTable, ImportTable:
	default:
		return nil, xerrors.Errorf("unknown gpfdist mode '%s'", mode)
	}

	tmpDir, err := os.MkdirTemp("", "gpfdist_")
	if err != nil {
		return nil, xerrors.Errorf("unable to create temp dir: %w", err)
	}
	host, err := resolveHostname()
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve hostname: %w", err)
	}
	gpfdist := &Gpfdist{
		cmd:           exec.Command(params.GpfdistBinPath, "-d", tmpDir, "-p", fmt.Sprint(minPort), "-P", fmt.Sprint(maxPort), "-w", "10"),
		host:          host,
		workingDir:    tmpDir,
		serviceSchema: params.ServiceSchema,
		ddlExecutor:   NewGpfdistDDLExecutor(conn),
		pipeName:      fmt.Sprintf("pipe-%s", terryid.GenerateSuffix()),
		mode:          mode,
		port:          0,
	}
	if err := gpfdist.initPipe(); err != nil {
		return nil, xerrors.Errorf("unable to init pipe: %w", err)
	}

	if err := gpfdist.startCmd(); err != nil {
		return nil, xerrors.Errorf("unable to start gpfdist: %w", err)
	}
	return gpfdist, nil
}

func resolveHostname() (string, error) {
	if host := os.Getenv("YT_IP_ADDRESS_DEFAULT"); host != "" {
		return fmt.Sprintf("[%s]", host), nil
	}
	return os.Hostname()
}

func (g *Gpfdist) startCmd() error {
	portChannel := make(chan int, 1)
	stderr, err := g.cmd.StderrPipe()
	if err != nil {
		return xerrors.Errorf("unable to get stderr pipe: %w", err)
	}
	go processLog(stderr, log.ErrorLevel, nil)

	stdout, err := g.cmd.StdoutPipe()
	if err != nil {
		return xerrors.Errorf("unable to get stdout pipe: %w", err)
	}
	go processLog(stdout, log.InfoLevel, portChannel)

	logger.Log.Debugf("Will start gpfdist command")
	if err = g.cmd.Start(); err != nil {
		return err
	}
	timer := time.NewTimer(time.Minute)
	select {
	case port := <-portChannel:
		g.port = port
		logger.Log.Debugf("Aquired port %d", g.port)
		return nil
	case <-timer.C:
		err := g.cmd.Process.Kill()
		if err != nil {
			logger.Log.Errorf("Can't kill process %v", err)
		}
		return xerrors.Errorf("unable to aquire gpfdist port number")
	}
}

func processLog(pipe io.ReadCloser, level log.Level, portChannel chan<- int) {
	var r *regexp.Regexp
	if portChannel != nil {
		r = regexp.MustCompile("^Serving HTTP on port ([0-9]+)[^0-9]+")
		defer func() {
			if portChannel != nil {
				close(portChannel)
			}
		}()
	}
	scanner := bufio.NewScanner(pipe)
	logger.Log.Infof("Start processing gpfdist %s level logs", level.String())
	for scanner.Scan() {
		line := scanner.Text()
		if portChannel != nil {
			matches := r.FindStringSubmatch(line)
			if len(matches) == 2 {
				port, err := strconv.Atoi(matches[1])
				if err != nil {
					logger.Log.Errorf("Error parsing port '%s': %v", matches[1], err)
				} else {
					portChannel <- port
				}
				close(portChannel)
				portChannel = nil
			}
		}
		switch level {
		case log.InfoLevel:
			logger.Log.Infof("Gpfdist: %s", line)
		default:
			logger.Log.Errorf("Gpfdist: %s", line)
		}
	}
	if scanner.Err() != nil {
		logger.Log.Errorf("Unable to read %s level logs string: %s", level, scanner.Err().Error())
	}
	logger.Log.Infof("Stopped processing gpfdist %s level logs", level.String())
}
