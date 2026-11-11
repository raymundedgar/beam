package transfer

import (
	"fmt"
	"log"
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
	buildID = buildRevision
	switch strings.ToLower(buildDirty) {
	case "1", "true", "dirty":
		if !strings.HasSuffix(buildID, "-dirty") {
			buildID += "-dirty"
		}
	}
}

func policyStamp() string {
	return fmt.Sprintf("build=%s read_median=%t read_restarts=%d put_policy=%t put_discards=%d put_late=%t early_put=%t",
		buildID, readMedian, readRestartCap, putPolicy, putDiscardCap(), putLateProbe, earlyPut)
}

var policyOnce sync.Once

func logPolicy() {
	policyOnce.Do(func() {
		log.Printf("transfer policy %s", policyStamp())
	})
}
