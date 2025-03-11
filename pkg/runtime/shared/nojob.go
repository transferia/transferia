package shared

import (
	"encoding/gob"
	"time"

	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/metrics"
)

func init() {
	gob.Register(new(JobNoJob))
}

type JobNoJob struct {
	Infinite bool
}

const catImage = `
                      /^--^\     /^--^\     /^--^\
                      \____/     \____/     \____/
                     /      \   /      \   /      \
KAT                 |        | |        | |        |
                     \__  __/   \__  __/   \__  __/
|^|^|^|^|^|^|^|^|^|^|^|^\ \^|^|^|^/ /^|^|^|^|^\ \^|^|^|^|^|^|^|^|^|^|^|^|
| | | | | | | | | | | | |\ \| | |/ /| | | | | | \ \ | | | | | | | | | | |
########################/ /######\ \###########/ /#######################
| | | | | | | | | | | | \/| | | | \/| | | | | |\/ | | | | | | | | | | | |
|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|
`

const catImageMirror = `

                      /^--^\     /^--^\     /^--^\
                      \____/     \____/     \____/
                     /      \   /      \   /      \
CAT                 |        | |        | |        |
                     \__  __/   \__  __/   \__  __/
|^|^|^|^|^|^|^|^|^|^|^| \ \^|^|^|^|\ \|^|^|^|/ /|^|^|^|^|^|^|^|^|^|^|^|^|
| | | | | | | | | | | |/ /| | | | |/ /| | | |\ \| | | | | | | | | | | | |
#######################\ \########/ /########/ /#########################
| | | | | | | | | | | | \/| | | | \/| | | | |\/ | | | | | | | | | | | | |
|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|_|
`

func (f *JobNoJob) Do(_ metrics.Registry) error {
	catTailDirectionToggle := 0
	logger.Log.Info(`
This job should do nothing.

Well, at least i'll draw some cats:
` + catImage)
	if f.Infinite {
		for {
			logger.Log.Info(renderCatImageWithJigglyWigglyTail(catTailDirectionToggle))
			time.Sleep(10 * time.Second)
			catTailDirectionToggle++
		}
	}
	return nil
}

func renderCatImageWithJigglyWigglyTail(catTailDirectionToggler int) string {
	currentCatImage := catImage
	if catTailDirectionToggler%2 == 0 {
		currentCatImage = catImageMirror
	}
	return currentCatImage
}
