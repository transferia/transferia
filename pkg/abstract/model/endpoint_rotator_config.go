package model

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/araddon/dateparse"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

// RotationTZ -- rotation is happened in preconfigured timezone, by default - Europe/Moscow
var RotationTZ *time.Location

func init() {
	var err error
	tz := "Europe/Moscow"
	if ttz, ok := os.LookupEnv("ROTATION_TZ"); ok {
		tz = ttz
	}
	if RotationTZ, err = time.LoadLocation(tz); err != nil {
		RotationTZ = time.Local
		logger.Log.Errorf("Couldn't initialize %s timezone. Using '%s' instead", tz, RotationTZ.String())
	}
}

type RotatorConfig struct {
	KeepPartCount     int
	PartType          RotatorPartType
	PartSize          int
	TimeColumn        string
	TableNameTemplate string
}

type RotatorPartType string
type dateBin time.Time    // date bin is represented in unique way with starting date
type partitionName string // name of partition depends on PartType (hours, days, months) and PartSize

const (
	RotatorPartHour  = RotatorPartType("h")
	RotatorPartDay   = RotatorPartType("d")
	RotatorPartMonth = RotatorPartType("m")
)

var (
	ErrRotatorConfigInvalidPartType      = errors.New("invalid part type")
	ErrRotatorConfigInvalidPartSize      = errors.New("part size should be positive (PartSize > 0)")
	ErrRotatorConfigInvalidKeepPartCount = errors.New("keep part count should be positive (KeepPartCount > 0)")
)

// TODO remove workaround when proper fix rolls out: YCDESIGN-1338 TM-1891
func (p *RotatorConfig) NilWorkaround() *RotatorConfig {
	if p == nil {
		return p
	}
	if p.KeepPartCount == 0 && p.PartSize == 0 {
		// struct with default values interpreted as nil config
		return nil
	}
	return p
}

func (p *RotatorConfig) Validate() error {
	if p == nil {
		return nil // nil is valid (no rotation applied in this case)
	}

	var errorList []error
	switch p.PartType {
	case RotatorPartHour, RotatorPartDay, RotatorPartMonth:
		break
	default:
		errorList = append(errorList, ErrRotatorConfigInvalidPartType)
	}
	if p.PartSize < 1 {
		errorList = append(errorList, ErrRotatorConfigInvalidPartSize)
	}
	if p.KeepPartCount < 1 {
		errorList = append(errorList, ErrRotatorConfigInvalidKeepPartCount)
	}
	if errorList != nil {
		return fmt.Errorf("rotator config errors occured: %v", errorList)
	}
	return nil
}

// breaks up months by partitions with size PartSize
// note, that if PartSize=5, the partitions would be uneven: [0,1,2,3,4], [5,6,7,8,9], [10,11]
func (p *RotatorConfig) getMonthPartitioned(m time.Month) time.Month {
	if p == nil {
		return m
	}
	// relies on January = iota + 1
	return time.Month(int(m) - (int(m)-int(time.January))%p.PartSize)
}

// this function calculates timestamp which belongs to partition with designated 'offset'
//
//	offset =  0 will keep date the same
//	offset =  1 will give out timestamp belonging to the next rotated partition
//	offset = -1 will give out timestamp belonging to the previous rotated partition
//	etc.
func (p *RotatorConfig) offsetDate(timestamp time.Time, offset int) time.Time {
	if p != nil {
		switch p.PartType {
		case RotatorPartHour:
			return timestamp.Add(time.Hour * time.Duration(p.PartSize*offset))
		case RotatorPartDay:
			return timestamp.AddDate(0, 0, p.PartSize*offset)
		case RotatorPartMonth:
			return timestamp.AddDate(0, p.PartSize*offset, 0)
		}
	}
	return timestamp
}

// getPartitionBin returns bin representative in which date is falling based on PartSize and PartType parameters
func (p *RotatorConfig) getPartitionBin(t time.Time) dateBin {
	if p != nil {
		switch p.PartType {
		case RotatorPartHour:
			return dateBin(time.Date(t.Year(), t.Month(), t.Day(), t.Hour()-(t.Hour()%p.PartSize), 0, 0, 0, t.Location()))
		case RotatorPartDay:
			return dateBin(time.Date(t.Year(), t.Month(), t.Day()-((t.Day()-1)%p.PartSize), 0, 0, 0, 0, t.Location()))
		case RotatorPartMonth:
			// if day != 1, then renormalization will shift down the month
			return dateBin(time.Date(t.Year(), p.getMonthPartitioned(t.Month()), 1, 0, 0, 0, 0, t.Location()))
		}
	}
	return dateBin(t)
}

// formatDate formats date according to PartType parameters
func (p *RotatorConfig) dateBinToPartitionName(db dateBin) partitionName {
	t := time.Time(db)
	if p != nil {
		switch p.PartType {
		case RotatorPartHour:
			return partitionName(t.Format(HourFormat))
		case RotatorPartDay:
			return partitionName(t.Format(DayFormat))
		case RotatorPartMonth:
			return partitionName(t.Format(MonthFormat))
		}
	}
	return partitionName(t.Format(MonthFormat))
}

// format function produces concrete formatted string given by name and partition
func (p *RotatorConfig) format(name string, partition partitionName) string {
	if p.TableNameTemplate == "" {
		return fmt.Sprintf("%v/%v", name, partition)
	}
	// TODO is it our problem if rotation specified by user invalid? What is 'invalid'?
	templ := strings.ReplaceAll(p.TableNameTemplate, "{{name}}", name)
	return strings.ReplaceAll(templ, "{{partition}}", string(partition))
}

// BaseTime defines TTL of tables w.r.t. KeepPartCount window size parameter
func (p *RotatorConfig) BaseTime() time.Time {
	return p.baseTime(time.Now())
}
func (p *RotatorConfig) baseTime(now time.Time) time.Time {
	if p != nil {
		switch p.PartType {
		case RotatorPartHour, RotatorPartDay, RotatorPartMonth:
			return p.offsetDate(now, -p.KeepPartCount)
		}
	}
	return time.Date(0, 0, 0, 0, 0, 0, 0, RotationTZ)
}

// AnnotateWithTime produces rotated name with custom time `v` provided as argument
func (p *RotatorConfig) AnnotateWithTime(name string, v time.Time) string {
	if p == nil {
		return name
	}
	// force use of time in RotationTZ
	rotationalTime := v.In(RotationTZ)
	// first of all, put date to it's bin
	reprDate := p.getPartitionBin(rotationalTime)
	// then convert the bin to rotated name
	partition := p.dateBinToPartitionName(reprDate)
	// format text according to user expectations
	return p.format(name, partition)
}

// Annotate is the default way to acquire rotated name based on the current time
// and structure settings: PartSize, PartType
func (p *RotatorConfig) Annotate(name string) string {
	return p.AnnotateWithTime(name, time.Now())
}

// Next returns rotated name with respect to current date
func (p *RotatorConfig) Next(name string) string {
	return p.AnnotateWithTime(name, p.offsetDate(time.Now(), 1))
}

// AnnotateWithTimeFromColumn gives rotated name according to column with name TimeColumn as the time source
//
// if column is absent, default Annotate method will be used
func (p *RotatorConfig) AnnotateWithTimeFromColumn(name string, item abstract.ChangeItem) string {
	if p == nil {
		return name
	}
	if p.TimeColumn != "" {
		partKey := ExtractTimeCol(item, p.TimeColumn)
		return p.AnnotateWithTime(name, partKey)
	} else {
		return p.Annotate(name)
	}
}

func ExtractTimeCol(item abstract.ChangeItem, timeColumn string) time.Time {
	partKey := time.Now()
	for i, k := range item.ColumnNames {
		if k == timeColumn {
			switch v := item.ColumnValues[i].(type) {
			case time.Time:
				partKey = v
			case *time.Time:
				if v != nil {
					partKey = *v
				}
			case string:
				t, err := dateparse.ParseAny(v)
				if err != nil {
					break
				}
				partKey = t
			case []byte:
				t, err := dateparse.ParseAny(string(v))
				if err != nil {
					break
				}
				partKey = t
			case *string:
				if v != nil {
					t, err := dateparse.ParseAny(*v)
					if err != nil {
						break
					}
					partKey = t
				}
			case int, int32, int64, uint, uint32, uint64:
				t, err := dateparse.ParseAny(fmt.Sprintf("%v", v))
				if err != nil {
					break
				}
				partKey = t
			}
			break
		}
	}
	return partKey
}

func (p *RotatorConfig) ParseTime(rotationTime string) (time.Time, error) {
	if p == nil {
		return time.Now(), xerrors.New("nil rotator in time parsing")
	}
	switch p.PartType {
	case RotatorPartHour:
		if len(rotationTime) == len(HourFormat) {
			t, err := time.ParseInLocation(HourFormat, rotationTime, RotationTZ)
			if err != nil {
				return time.Now(), xerrors.Errorf("cannot parse time %s in location %s: %w", rotationTime, RotationTZ.String(), err)
			}
			return t, nil
		}
	case RotatorPartDay:
		if len(rotationTime) == len(DayFormat) {
			t, err := time.ParseInLocation(DayFormat, rotationTime, RotationTZ)
			if err != nil {
				return time.Now(), xerrors.Errorf("cannot parse time %s in location %s: %w", rotationTime, RotationTZ.String(), err)
			}
			return t, nil
		}
	case RotatorPartMonth:
		if len(rotationTime) == len(MonthFormat) {
			t, err := time.ParseInLocation(MonthFormat, rotationTime, RotationTZ)
			if err != nil {
				return time.Now(), xerrors.Errorf("cannot parser time %s in location %s: %w", rotationTime, RotationTZ.String(), err)
			}
			return t, nil
		}
	}
	return time.Now(), xerrors.New("wrong format specified")
}
