//go:build pelican

// §32b: is the TOCTOU window in RenameNoReplace real, or only theoretical?
//
// The single-writer test (§32) passes 6/6 and reports "4 RenameNoReplace
// calls that could not be made atomic". That phrasing asserts a risk it did
// not actually demonstrate -- the lost-update claim is inherited from §18
// (the custom catalog) and §31 (no CAS primitive), not measured on THIS
// adapter. An unverified caveat attached to a passing test is exactly the
// kind of thing that gets dropped when someone skims the result, so it is
// worth either demonstrating or withdrawing.
//
// This drives the adapter's own RenameNoReplace concurrently onto one
// destination. A correct implementation lets exactly ONE caller win and
// returns fs.ErrExist to the rest. If more than one wins, the window is
// real and the "single writer only" constraint is load-bearing rather than
// cautionary.
package main

import (
	"fmt"
	"sync"
	"testing"
)

func TestRenameNoReplaceIsNotAtomic(t *testing.T) {
	key, err := loadKey(defaultKeyPath())
	if err != nil {
		t.Skipf("no Pelican key: %v", err)
	}
	const pre = "/cold-storage-test-fscat1"
	hfs := &hadoopFS{PelicanIO: NewPelicanIO(key, pre)}
	t.Cleanup(func() {
		n, _ := hfs.PelicanIO.RemoveAll(pre)
		st, _, _ := hfs.PropFind(pre)
		t.Logf("cleanup: %d objects removed, PROPFIND -> %d", n, st)
	})

	const n = 4
	dst := pre + "/race/committed.json"

	// Each writer stages its own distinct source, then all race to claim dst.
	for i := 0; i < n; i++ {
		if err := hfs.WriteFile(fmt.Sprintf("%s/race/src%d", pre, i),
			[]byte(fmt.Sprintf("writer-%d", i))); err != nil {
			t.Fatalf("staging src%d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	winners, refused := []int{}, 0
	start := make(chan struct{})

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start // release them together, to actually contend
			err := hfs.RenameNoReplace(fmt.Sprintf("%s/race/src%d", pre, id), dst)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				winners = append(winners, id)
			} else {
				refused++
			}
		}(i)
	}
	close(start)
	wg.Wait()

	final, ferr := hfs.ReadFile(dst)
	t.Logf("%d concurrent RenameNoReplace onto one destination", n)
	t.Logf("  reported success: %v", winners)
	t.Logf("  refused with ErrExist: %d", refused)
	t.Logf("  destination finally holds: %q (err=%v)", string(final), ferr)

	switch {
	case len(winners) == 1:
		t.Logf("[PASS] exactly one winner -- the window did not open on this run")
		t.Logf("       NB: this is timing luck, not a guarantee. §31 showed the " +
			"origin cannot express a conditional rename, so nothing SERIALIZES " +
			"these; a single-winner run does not make the adapter safe.")
	case len(winners) > 1:
		t.Logf("[DEMONSTRATED] %d writers all believed they won, and the "+
			"destination holds only one of them -- the other %d commits were "+
			"acknowledged and silently lost. This is the lost update §24 "+
			"predicted, now measured on this adapter rather than inferred.",
			len(winners), len(winners)-1)
	default:
		t.Errorf("[FAIL] no writer won at all (%d refused) -- unexpected, "+
			"the destination started free", refused)
	}
}
