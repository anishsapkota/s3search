package index

import (
	"errors"
	"fmt"
	"time"
)

func newSegmentID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func isAs(err error, target interface{}) bool {
	type iface interface{ As(interface{}) bool }
	return errors.As(err, target)
}
