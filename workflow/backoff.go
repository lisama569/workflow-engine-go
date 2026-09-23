package workflow

import "time"

// ConstantBackoff 固定间隔退避。
type ConstantBackoff struct{ Interval time.Duration }

func (b ConstantBackoff) Next(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	return b.Interval
}

// LinearBackoff 线性退避：Interval * attempt。
type LinearBackoff struct{ Interval time.Duration }

func (b LinearBackoff) Next(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	return b.Interval * time.Duration(attempt)
}

// NoBackoff 无退避。
type NoBackoff struct{}

func (b NoBackoff) Next(attempt int) time.Duration { return 0 }
