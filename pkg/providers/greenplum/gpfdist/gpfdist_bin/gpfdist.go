package gpfdistbin

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/library/go/core/xerrors/multierr"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/terryid"
	"go.ytsaurus.tech/library/go/core/log"
	"golang.org/x/sync/errgroup"
)

const (
	openFifoTimeout = 600 * time.Second
	defaultPipeMode = uint32(0644)
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
	pipes         []string
	ddlExecutor   *GpfdistDDLExecutor
	mode          GpfdistMode
}

func (g *Gpfdist) Stop() error {
	var errors []error
	if err := g.removePipes(); err != nil {
		errors = append(errors, xerrors.Errorf("unable to remove pipes: %w", err))
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

func (g *Gpfdist) OpenPipes() ([]*os.File, error) {
	files := make([]*os.File, len(g.pipes))
	eg := errgroup.Group{}
	for i, pipe := range g.pipes {
		eg.Go(func() error {
			var err error
			files[i], err = g.openPipe(pipe, g.pipeOpenFlag())
			return err
		})
	}
	if err := eg.Wait(); err != nil {
		for _, file := range files {
			if file == nil {
				continue
			}
			if closeErr := file.Close(); closeErr != nil {
				logger.Log.Error(fmt.Sprintf("Unable to close file %s", file.Name()), log.Error(err))
			}
		}
		return nil, err
	}
	return files, nil
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

func (g *Gpfdist) openPipe(name string, openFlag int) (*os.File, error) {
	var cancelFlag int
	switch openFlag {
	case os.O_RDONLY:
		cancelFlag = os.O_WRONLY | syscall.O_NONBLOCK
	case os.O_WRONLY:
		cancelFlag = os.O_RDONLY | syscall.O_NONBLOCK
	}

	pipePath := g.fullPath(name)
	var file *os.File
	openFile := func() error {
		var openErr error
		file, openErr = os.OpenFile(pipePath, openFlag, 0)
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
		return nil, xerrors.Errorf("unable to open pipe %s file: %w", name, err)
	}
	return file, nil
}

// fullPath concatenates working directory and "/" to the left of provided relative path.
func (g *Gpfdist) fullPath(relativePath string) string {
	return fmt.Sprintf("%s/%s", g.workingDir, relativePath)
}

func (g *Gpfdist) locations() []string {
	res := make([]string, len(g.pipes))
	for i, pipe := range g.pipes {
		res[i] = fmt.Sprintf("gpfdist://%s:%d/%s", g.host, g.port, pipe)
	}
	return res
}

func (g *Gpfdist) removePipes() error {
	var errors []error
	for _, pipe := range g.pipes {
		fullPath := g.fullPath(pipe)
		logger.Log.Infof("Removing pipe %s", fullPath)
		if err := os.Remove(fullPath); err != nil {
			errors = append(errors, err)
		}
	}
	return multierr.Combine(errors...)
}

func (g *Gpfdist) initPipes(n int) error {
	g.pipes = make([]string, n)
	prefix := fmt.Sprintf("pipe-%s", terryid.GenerateSuffix())
	for i := range n {
		name := fmt.Sprintf("%s-%d", prefix, i)
		fullPath := g.fullPath(name)
		logger.Log.Infof("Creating pipe %s", fullPath)
		if err := syscall.Mkfifo(fullPath, defaultPipeMode); err != nil {
			return xerrors.Errorf("unable to create pipe %s: %w", fullPath, err)
		}
		g.pipes[i] = name
	}
	return nil
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
	host := params.Host
	if host == "" {
		if host, err = resolveHostname(); err != nil {
			return nil, xerrors.Errorf("unable to resolve hostname: %w", err)
		}
	}
	port := params.Port
	if port == 0 {
		port = 8500 // TODO: Map port in [8500; 8600) range if port is in use (e.g. by other instance of gpfdist).
	}
	pipesCnt := params.PipesCnt
	if pipesCnt < 1 {
		pipesCnt = 1
	}

	gpfdist := &Gpfdist{
		cmd:           exec.Command(params.GpfdistBinPath, "-d", tmpDir, "-p", fmt.Sprint(port), "-w", "10"),
		host:          host,
		port:          port,
		workingDir:    tmpDir,
		serviceSchema: params.ServiceSchema,
		ddlExecutor:   NewGpfdistDDLExecutor(conn),
		pipes:         nil,
		mode:          mode,
	}
	if err := gpfdist.initPipes(pipesCnt); err != nil {
		return nil, xerrors.Errorf("unable to init pipes: %w", err)
	}

	if err := startCmd(gpfdist.cmd); err != nil {
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

func startCmd(cmd *exec.Cmd) error {
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return xerrors.Errorf("unable to get stderr pipe: %w", err)
	}
	go processLog(stderr, log.ErrorLevel)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return xerrors.Errorf("unable to get stdout pipe: %w", err)
	}
	go processLog(stdout, log.InfoLevel)

	logger.Log.Debugf("Will start gpfdist command")
	return cmd.Start()
}

func processLog(pipe io.ReadCloser, level log.Level) {
	scanner := bufio.NewScanner(pipe)
	logger.Log.Infof("Start processing gpfdist %s level logs", level.String())
	for scanner.Scan() {
		line := scanner.Text()
		switch level {
		case log.InfoLevel:
			// logger.Log.Infof("Gpfdist: %s", line)
		default:
			logger.Log.Errorf("Gpfdist: %s", line)
		}
	}
	if scanner.Err() != nil {
		logger.Log.Errorf("Unable to read %s level logs string: %s", level, scanner.Err().Error())
	}
	logger.Log.Infof("Stopped processing gpfdist %s level logs", level.String())
}
