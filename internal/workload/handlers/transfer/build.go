package transfer

import (
	"fmt"
	"log"
	"runtime/debug"
	"strings"
	"sync"
)

// buildRevision and buildDirty are set with -ldflags at link time.
// A dirty tree is reported so a wave can be tied to uncommitted code.
var (
	buildRevision = "unknown"
	buildDirty    = ""
	buildID       = "unknown"
)

func init() {
	buildID = strings.TrimSpace(buildRevision)
	if buildID == "" || buildID == "unknown" {
		if rev := revisionFromBuildInfo(); rev != "" {
			buildID = rev
		}
	}
	if buildID == "" {
		buildID = "unknown"
	}
	switch strings.ToLower(buildDirty) {
	case "1", "true", "dirty":
		if !strings.HasSuffix(buildID, "-dirty") {
			buildID += "-dirty"
		}
	}
}

// revisionFromBuildInfo reads the VCS settings Go embeds when the binary is
// built inside a git checkout. The ldflag wins when it is set.
func revisionFromBuildInfo() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	var rev, modified string
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			rev = setting.Value
		case "vcs.modified":
			modified = setting.Value
		}
	}
	if rev == "" {
		return ""
	}
	if len(rev) > 7 {
		rev = rev[:7]
	}
	if modified == "true" {
		rev += "-dirty"
	}
	return rev
}

func policyStamp() string {
	return fmt.Sprintf("build=%s read_median=%t read_probe_replacements=%d read_restarts=%d read_keep_mib=%d http_attempts=%d read_race=%t reset_no_etag=%t put_policy=%t put_discards=%d put_hold=%s put_hold_resp=%s put_etag_strict=%t put_late=%t early_put=%t",
		buildID, readMedian, maxSourceReplacements, readRestartCap, readKeepMinRate>>20, presignedAttempts, readRace, readResetNoETag, putPolicy, putDiscardCap(), putHoldCap, putHoldResponseCap, putETagStrict, putLateProbe, earlyPut)
}

var policyOnce sync.Once

func logPolicy() {
	policyOnce.Do(func() {
		log.Printf("transfer policy %s", policyStamp())
	})
}
