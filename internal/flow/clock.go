package flow

import "time"

type Clock func() time.Time

var systemClock Clock = time.Now

func nowTime() time.Time { return systemClock() }
func nowUnixNano() int64 { return systemClock().UnixNano() }
