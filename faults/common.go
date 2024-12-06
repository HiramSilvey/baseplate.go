package faults

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"golang.org/x/exp/rand"
)

type GetHeaderFn func(key string) string
type ResumeFn func() (interface{}, error)
type ResponseFn func(code int, message string) (interface{}, error)
type SleepFn func(d time.Duration)

type randSingleton struct {
	randInt *int
}

func (r randSingleton) getRandInt() int {
	if r.randInt == nil {
		*r.randInt = rand.Intn(100)
	}
	return *r.randInt
}

func isSelected(percentageHeader string, GetHeaderFn func(key string) string, singleRand randSingleton) (bool, string) {
	percentageStr := GetHeaderFn(percentageHeader)
	if percentageStr == "" {
		return true, ""
	}
	percentage, err := strconv.Atoi(percentageStr)
	if err != nil {
		return false, fmt.Sprintf("provided delay percentage %s is not a valid integer", percentageStr)
	}
	if percentage < 0 || percentage > 100 {
		return false, fmt.Sprintf("provided delay percentage %d is outside the valid range of [0-100]", percentage)
	}
	return singleRand.getRandInt() < percentage, ""
}

type InjectFaultParams struct {
	CallerName string

	Address, Method            string
	AbortCodeMin, AbortCodeMax int

	GetHeaderFn GetHeaderFn
	ResumeFn    ResumeFn
	ResponseFn  ResponseFn

	// Exposed for tests
	RandInt *int
	SleepFn *SleepFn
}

func InjectFault(params InjectFaultParams) (interface{}, error) {
	serverAddress := params.GetHeaderFn(FaultServerAddressHeader)
	if serverAddress == "" || serverAddress != strings.TrimSuffix(params.Address, ".svc.cluster.local") {
		return params.ResumeFn()
	}

	serverMethod := params.GetHeaderFn(FaultServerMethodHeader)
	if serverMethod != "" && serverMethod != params.Method {
		return params.ResumeFn()
	}

	singleRand := randSingleton{
		randInt: params.RandInt,
	}

	delayMs := params.GetHeaderFn(FaultDelayMsHeader)
	if delayMs != "" {
		if selected, msg := isSelected(FaultDelayPercentageHeader, params.GetHeaderFn, singleRand); !selected {
			slog.Warn(fmt.Sprintf("%s: %s", params.CallerName, msg))
			return params.ResumeFn()
		}

		delay, err := strconv.Atoi(delayMs)
		if err != nil {
			slog.Warn(fmt.Sprintf("%s: provided delay %s is not a valid integer", params.CallerName, delayMs))
			return params.ResumeFn()
		}

		sleepFn := time.Sleep
		if params.SleepFn != nil {
			sleepFn = *params.SleepFn
		}
		sleepFn(time.Duration(delay) * time.Millisecond)
	}

	abortCode := params.GetHeaderFn(FaultAbortCodeHeader)
	if abortCode != "" {
		if selected, msg := isSelected(FaultAbortPercentageHeader, params.GetHeaderFn, singleRand); !selected {
			slog.Warn(fmt.Sprintf("%s: %s", params.CallerName, msg))
			return params.ResumeFn()
		}

		code, err := strconv.Atoi(abortCode)
		if err != nil {
			slog.Warn(fmt.Sprintf("%s: provided abort code %s is not a valid integer", params.CallerName, abortCode))
			return params.ResumeFn()
		}
		if code < params.AbortCodeMin || code > params.AbortCodeMax {
			slog.Warn(fmt.Sprintf("%s: provided abort code %d is outside of the valid range", params.CallerName, code))
			return params.ResumeFn()
		}
		abortMessage := params.GetHeaderFn(FaultAbortMessageHeader)
		return params.ResponseFn(code, abortMessage)
	}

	return params.ResumeFn()
}
