package gk

import (
	//"bufio"
	"fmt"
  "math"
  "net"
  "time"
)

type item struct {
  value int64
  collapseCount int
  delta int
  next *item
}

type gk struct {
  epsilon float64
  compact_size int
  totalCount int
  sampleCount int
  head *item
}

func NewGK (epsilon float64, compact_size int) *gk {
  g := new(gk)
  g.totalCount = 0
  g.epsilon = epsilon
  g.compact_size = compact_size
  return g
}

func NewItem (value int64) *item {
  i := new(item)
  i.value = value
  i.collapseCount = 1
  return i
}

func nilItem() *item {
	return nil
}

/* insert value into gk underlying data structure,
 * using the Greenwald-Khanna method for processing
 * streaming data for percentile estimation.
 *
 * This code was influenced by the code in this project
 * https://github.com/umbrant/QuantileEstimation, written
 * in Java
 *
 */
func (g *gk) Insert (value int64) error {
  g.totalCount++
  g.sampleCount++
  item := NewItem(value)
  d := int(math.Floor(2 * g.epsilon * float64(g.totalCount)))

  head := g.head
  if head == nil {
    g.head = item
    return nil
  } else if (head.value > value) {
    item.next = head
    g.head = item
  } else {
    prv := head
    tmp := head.next
    for tmp != nil {
      next := tmp.next
      if (tmp.value > value) {
        prv.next = item
        item.next = tmp
        item.delta = d
        break
      }
      prv = tmp
      tmp = next
    }

    if tmp == nil {
      prv.next = item
    }
  }

  if (g.sampleCount > g.compact_size) {
    s := nilItem()
    tmp := g.head
    next := tmp.next

    for next != nil {
      total_collapseCount := tmp.collapseCount + next.collapseCount
      if (total_collapseCount + next.delta <= d) {
        g.sampleCount -= 1
        next.collapseCount = total_collapseCount

        if s == nil{
          g.head = next
        } else {
          s.next = next
        }
      } else {
        s = tmp
      }

      tmp = next
      next = tmp.next
    }
  }
  return nil
}

/* For each percentile in percentiles
 * write message to tcp socket at address percentileSocketAddr
 * with that percentile and the estimated corresponding value.
 *
 * Estimates are generated using the Greenwald-Khanna method
 * for querying an individual percentile value from the
 * underlying data structure of g
 *
 * This code was influenced by the code in this project
 * https://github.com/umbrant/QuantileEstimation, written
 * in Java
 */
func (g *gk) Report (percentiles []int, percentileSocketAddr string) error {
  conn, _ := net.Dial("tcp", percentileSocketAddr)
  totalCount := g.totalCount

	tmp2 := g.head

	for tmp2 != nil {
		tmp2 = tmp2.next
	}

  if totalCount == 0 {
      return nil
  }

  num_percentiles := len(percentiles)
  thresholds := make([]float64, num_percentiles)
  res := make([]int64, num_percentiles)
  t_idx := 0

  for idx, p := range percentiles {
    thresholds[idx] = float64(p * totalCount / 100)
  }

  tmp := g.head
  rankMin := 0

  d := 2 * g.epsilon * float64(totalCount)

  threshold := thresholds[t_idx]
  prev := g.head
  tmp = prev.next
  for tmp != nil {
    rankMin += prev.collapseCount

    for t_idx < num_percentiles {
      threshold = thresholds[t_idx]
      if (float64(rankMin + tmp.collapseCount + tmp.delta) >
			 (threshold + d)) {
        res[t_idx] = prev.value

        t_idx++
        break
      } else {
        break
      }
    }

    if t_idx == num_percentiles {
      break
    }

    prev = tmp
    tmp = tmp.next
  }

  max_val := prev.value
  for i := t_idx; i < num_percentiles; i++ {
    res[i] = max_val
  }

  now := time.Now().Unix()
	fmt.Println("starting")
  for i := 0; i < num_percentiles; i++ {
		//message, _ := bufio.NewReader(conn).ReadString('\n')
    _, err := conn.Write([]byte(fmt.Sprintf("file_size_p%d %d %d\n",
			 percentiles[i], res[i], now)))
		if err != nil {
			fmt.Println("err!!")
		}
  }

  return nil
}
