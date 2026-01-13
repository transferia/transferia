package util

import (
	"os"
	"strconv"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/core/log"
)

// YT has 2 different variables: YT_JOB_INDEX & YT_JOB_COOKIE:
//
//
// YT_JOB_INDEX
// doc:
//     This is the ordinal number of the running job within the task.
//     It may be greater than job_count if some jobs failed and were restarted.
//     It is specified in the operation specification and is not saved when the incarnation is restarted.
//
// YT_JOB_COOKIE
// doc:
//     A unique identifier assigned to a job that persists even after a job restart or reincarnation.
//     Used as a reliable way to identify a job, especially in distributed scenarios such as gang operations.//
//
//
// In data-transfer we have entity 'job_index'.
// But over 'job_index' we assume: YT_JOB_COOKIE

func JobOrdinal() (int, error) {
	ytJobCookieStr := os.Getenv("YT_JOB_COOKIE")
	if ytJobCookieStr == "" {
		return 0, xerrors.New("env YT_JOB_COOKIE is empty string")
	}
	ytJobCookie, err := strconv.Atoi(ytJobCookieStr)
	if err != nil {
		return 0, xerrors.Errorf("failed to convert YT_JOB_COOKIE to int, ytJobCookie: %s, err: %w", ytJobCookieStr, err)
	}
	return ytJobCookie, nil
}

func LogYtJobIndexAndCookie(logger log.Logger) {
	ytJobIndexStr := os.Getenv("YT_JOB_INDEX")
	ytJobCookieStr := os.Getenv("YT_JOB_COOKIE")
	logger.Infof("YT_JOB_INDEX=%s", ytJobIndexStr)
	logger.Infof("YT_JOB_COOKIE=%s", ytJobCookieStr)
}
