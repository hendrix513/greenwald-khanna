package gk

import (
	"testing"
)

/* Incomplete test- should also verify that internal sample list of GK
 * is updated correct when compactSize is exceeded
 */
func TestInsert(t *testing.T) {
  compactSize := 3
  epsilon := .05
  gk := NewGK(epsilon, compactSize)

  gk.Insert(3)
  gk.Insert(1)
  gk.Insert(2)

  tmp := gk.head
  if tmp.value != 1 || tmp.collapseCount != 1 {
    t.Errorf("Insert incorrect")
  }

  tmp = tmp.next
  if tmp.value != 2 || tmp.collapseCount != 1 {
    t.Errorf("Insert incorrect")
  }

  tmp = tmp.next
  if tmp.value != 3 || tmp.collapseCount != 1 {
    t.Errorf("Insert incorrect")
  }

  tmp = tmp.next
  if tmp != nil {
    t.Errorf("Insert incorrect")
  }
}

/* Test stub- should create GK instances each with various values for
 * its internal linked list map and verify that calling Report with various
 * lists of percentile values sends a properly formatted message to the correct
 * tcp socket address for each percentile value
 *
 */
func Report(t *testing.T) {
}
