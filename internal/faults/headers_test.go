package faults

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParsePercentage(t *testing.T) {
	testCases := []struct {
		name       string
		percentage string
		want       int
		wantErr    string
	}{
		{
			name:       "empty",
			percentage: "",
			want:       100,
		},
		{
			name:       "valid",
			percentage: "50",
			want:       50,
		},
		{
			name:       "NaN",
			percentage: "NaN",
			want:       0,
			wantErr:    "not a valid integer",
		},
		{
			name:       "under min",
			percentage: "-1",
			want:       0,
			wantErr:    "outside the valid range",
		},
		{
			name:       "over max",
			percentage: "101",
			want:       0,
			wantErr:    "outside the valid range",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parsePercentage(tc.percentage)
			if got != tc.want {
				t.Fatalf("expected %v, got %v", tc.want, got)
			}
			if tc.wantErr == "" && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error to contain %q, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestParseMatchingFaultHeader(t *testing.T) {
	testCases := []struct {
		name             string
		headerValue      string
		canonicalAddress string
		method           string
		abortCodeMin     int
		abortCodeMax     int
		want             *faultConfiguration
		wantErr          error
	}{
		{
			name:        "empty",
			headerValue: "",
		},
		{
			name:        "missing server address",
			headerValue: "m=bar",
		},
		{
			name:             "basic valid",
			headerValue:      "a=foo",
			canonicalAddress: "foo",
			want: &faultConfiguration{
				ServerAddress:   "foo",
				DelayPercentage: 100,
				AbortCode:       -1,
				AbortPercentage: 100,
			},
		},
		{
			name:             "full valid",
			headerValue:      "a=foo;m=bar;d=100;D=50;f=500;b=Fault injected!;F=75",
			canonicalAddress: "foo",
			method:           "bar",
			abortCodeMax:     599,
			want: &faultConfiguration{
				ServerAddress:   "foo",
				ServerMethod:    "bar",
				DelayMs:         100,
				DelayPercentage: 50,
				AbortCode:       500,
				AbortMessage:    "Fault injected!",
				AbortPercentage: 75,
			},
		},
		{
			name:        "invalid key-value pair",
			headerValue: "foo",
			wantErr:     &errKVPairInvalid{"foo"},
		},
		{
			name:             "server address does not match",
			headerValue:      "a=foo",
			canonicalAddress: "bar",
		},
		{
			name:             "method does not match",
			headerValue:      "a=foo;m=bar",
			canonicalAddress: "foo",
			method:           "baz",
		},
		{
			name:             "invalid delay value",
			headerValue:      "a=foo;d=NaN",
			canonicalAddress: "foo",
			wantErr:          errDelayInvalid,
		},
		{
			name:             "invalid delay percentage",
			headerValue:      "a=foo;D=NaN",
			canonicalAddress: "foo",
			wantErr:          &errPercentageInvalidInt{"NaN"},
		},
		{
			name:             "invalid delay percentage negative",
			headerValue:      "a=foo;D=-1",
			canonicalAddress: "foo",
			wantErr:          &errPercentageOutOfRange{-1},
		},
		{
			name:             "invalid delay percentage over 100",
			headerValue:      "a=foo;D=101",
			canonicalAddress: "foo",
			wantErr:          &errPercentageOutOfRange{101},
		},
		{
			name:             "invalid abort code value",
			headerValue:      "a=foo;f=NaN",
			canonicalAddress: "foo",
			wantErr:          errAbortCodeInvalid,
		},
		{
			name:             "invalid abort code below minimum",
			headerValue:      "a=foo;f=399",
			canonicalAddress: "foo",
			abortCodeMin:     400,
			abortCodeMax:     599,
			wantErr:          &errAbortCodeOutOfRange{399, 400, 599},
		},
		{
			name:             "invalid abort code above maximum",
			headerValue:      "a=foo;f=600",
			canonicalAddress: "foo",
			abortCodeMin:     400,
			abortCodeMax:     599,
			wantErr:          &errAbortCodeOutOfRange{600, 400, 599},
		},
		{
			name:             "invalid abort percentage",
			headerValue:      "a=foo;F=NaN",
			canonicalAddress: "foo",
			wantErr:          &errPercentageInvalidInt{"NaN"},
		},
		{
			name:             "invalid abort percentage negative",
			headerValue:      "a=foo;F=-1",
			canonicalAddress: "foo",
			wantErr:          &errPercentageOutOfRange{-1},
		},
		{
			name:             "invalid abort percentage over 100",
			headerValue:      "a=foo;F=101",
			canonicalAddress: "foo",
			wantErr:          &errPercentageOutOfRange{101},
		},
		{
			name:        "invalid key",
			headerValue: "foo=bar",
			wantErr:     &errUnknownKey{"foo"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseMatchingFaultHeader(tc.headerValue, tc.canonicalAddress, tc.method, tc.abortCodeMin, tc.abortCodeMax)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected error %v, got %v", tc.wantErr, err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Fatalf("fault mismatch: (-want +got)\n%s", diff)
			}
		})
	}
}

func TestParsingFaultConfiguration(t *testing.T) {
	testCases := []struct {
		name             string
		headerValues     []string
		canonicalAddress string
		want             *faultConfiguration
		wantErr          string
	}{
		{
			name:         "empty",
			headerValues: []string{},
		},
		{
			name:             "single valid match",
			headerValues:     []string{"a=foo"},
			canonicalAddress: "foo",
			want: &faultConfiguration{
				ServerAddress:   "foo",
				DelayPercentage: 100,
				AbortCode:       -1,
				AbortPercentage: 100,
			},
		},
		{
			name:             "multiple valid match",
			headerValues:     []string{"a=bar", "a=baz, a=foo"},
			canonicalAddress: "foo",
			want: &faultConfiguration{
				ServerAddress:   "foo",
				DelayPercentage: 100,
				AbortCode:       -1,
				AbortPercentage: 100,
			},
		},
		{
			name:             "multiple valid no match",
			headerValues:     []string{"a=bar", "a=foo, a=quux"},
			canonicalAddress: "baz",
		},
		{
			name:         "single invalid",
			headerValues: []string{"foo"},
			wantErr:      "invalid key-value pair",
		},
		{
			name:         "multiple invalid",
			headerValues: []string{"foo", "bar, baz"},
			wantErr:      "invalid key-value pair: \"foo\", invalid key-value pair: \"bar\", invalid key-value pair: \"baz\"",
		},
		{
			name:             "mixed validity match",
			headerValues:     []string{"foo", "a=bar, baz"},
			canonicalAddress: "bar",
			want: &faultConfiguration{
				ServerAddress:   "bar",
				DelayPercentage: 100,
				AbortCode:       -1,
				AbortPercentage: 100,
			},
			wantErr: "invalid key-value pair: \"foo\"",
		},
	}

	testMethod := ""
	testAbortCodeMin, testAbortCodeMax := 0, 0

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseMatchingFaultConfiguration(tc.headerValues, tc.canonicalAddress, testMethod, testAbortCodeMin, testAbortCodeMax)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected an error, got nil")
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error to contain %q, got %v", tc.wantErr, err)
				}
			}
			if tc.want != nil {
				if got == nil {
					t.Fatalf("expected fault configuration, got nil")
				}
				if got.ServerAddress != tc.want.ServerAddress {
					t.Fatalf("expected server address %q, got %q", tc.want.ServerAddress, got.ServerAddress)
				}
				if got.ServerMethod != tc.want.ServerMethod {
					t.Fatalf("expected server method %q, got %q", tc.want.ServerMethod, got.ServerMethod)
				}
				if got.DelayMs != tc.want.DelayMs {
					t.Fatalf("expected delay %d, got %d", tc.want.DelayMs, got.DelayMs)
				}
				if got.DelayPercentage != tc.want.DelayPercentage {
					t.Fatalf("expected delay percentage %d, got %d", tc.want.DelayPercentage, got.DelayPercentage)
				}
				if got.AbortCode != tc.want.AbortCode {
					t.Fatalf("expected abort code %d, got %d", tc.want.AbortCode, got.AbortCode)
				}
				if got.AbortMessage != tc.want.AbortMessage {
					t.Fatalf("expected abort message %q, got %q", tc.want.AbortMessage, got.AbortMessage)
				}
				if got.AbortPercentage != tc.want.AbortPercentage {
					t.Fatalf("expected abort percentage %d, got %d", tc.want.AbortPercentage, got.AbortPercentage)
				}
			}
		})
	}
}
