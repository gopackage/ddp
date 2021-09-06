package ddp_test

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/gopackage/ddp"
	"github.com/stretchr/testify/assert"
)

func TestTime(t *testing.T) {
	current := ddp.Time{Time: time.Date(2021, time.October, 31, 1, 2, 3, 4, time.UTC)}
	golden := "{\"$date\":"+strconv.FormatInt(current.UnixMilli(), 10)+"}"

	bytes, err := json.Marshal(current)
	assert.Nil(t, err, "marshal failed")
	assert.Equal(t, golden, string(bytes))

	err = json.Unmarshal([]byte(golden), &current)
	assert.Nil(t, err, "unmarshal failed")
	assert.Equal(t, 2021, current.Year())
	assert.Equal(t, time.October, current.Month())
	assert.Equal(t, 31, current.Day())
	assert.Equal(t, 1, current.Hour())
	assert.Equal(t, 2, current.Minute())
	assert.Equal(t, 0, current.Nanosecond()) // EJSON truncates nanoseconds
}
