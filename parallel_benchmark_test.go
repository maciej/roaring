package roaring

import (
	"math/rand"
	"runtime"
	"testing"
)

type parOp int

const (
	parOpAnd parOp = iota
	parOpOr
	parOpXor
	parOpAndNot
)

var defaultWorkerCount int = runtime.NumCPU()

var GlobalParAggregator = NewParAggregator()

// TODO figure out what would be a good default
const defaultTaskQueueLength = 4096

type ParAggregator struct {
	taskQueue chan parTask
}

// TODO parameterize with task queue length and number of workers
func NewParAggregator() ParAggregator {
	agg := ParAggregator{
		make(chan parTask, defaultTaskQueueLength),
	}

	for i := 0; i < defaultWorkerCount; i++ {
		go agg.worker()
	}

	return agg
}

func (aggregator ParAggregator) worker() {
	for task := range aggregator.taskQueue {
		var resultContainer container
		switch task.op {
		case parOpAnd:
			resultContainer = task.left.and(task.right)
		}

		result := parResult{
			key: task.key,
			pos: task.pos,
		}

		if resultContainer.getCardinality() > 0 {
			result.container = resultContainer
		} else {
			result.empty = true
		}

		task.result <- result
	}
}

func (aggregator ParAggregator) Shutdown() {
	close(aggregator.taskQueue)
}

// TODO check if using pointer types for containers is possible
// As I understand Golang this would result in one less copy
type parTask struct {
	op          parOp
	key         uint16
	pos         int
	left, right container
	result      chan<- parResult
}

type parResult struct {
	key       uint16
	pos       int
	container container
	empty     bool
}

func (aggregator ParAggregator) And(x1, x2 *Bitmap) *Bitmap {
	answer := NewBitmap()
	pos1 := 0
	pos2 := 0
	length1 := x1.highlowcontainer.size()
	length2 := x2.highlowcontainer.size()

	resultChan := make(chan parResult, 64)
	resultCount := 0

main:
	for pos1 < length1 && pos2 < length2 {
		s1 := x1.highlowcontainer.getKeyAtIndex(pos1)
		s2 := x2.highlowcontainer.getKeyAtIndex(pos2)
		for {
			if s1 == s2 {
				C := x1.highlowcontainer.getContainerAtIndex(pos1)

				aggregator.taskQueue <- parTask{
					op:     parOpAnd,
					pos:    resultCount,
					left:   C,
					right:  x2.highlowcontainer.getContainerAtIndex(pos2),
					result: resultChan,
				}
				resultCount++

				pos1++
				pos2++
				if (pos1 == length1) || (pos2 == length2) {
					break main
				}
				s1 = x1.highlowcontainer.getKeyAtIndex(pos1)
				s2 = x2.highlowcontainer.getKeyAtIndex(pos2)
			} else if s1 < s2 {
				pos1 = x1.highlowcontainer.advanceUntil(s2, pos1)
				if pos1 == length1 {
					break main
				}
				s1 = x1.highlowcontainer.getKeyAtIndex(pos1)
			} else { // s1 > s2
				pos2 = x2.highlowcontainer.advanceUntil(s1, pos2)
				if pos2 == length2 {
					break main
				}
				s2 = x2.highlowcontainer.getKeyAtIndex(pos2)
			}
		}
	}
	// main loop end

	results := make([]parResult, resultCount)

	for result := range resultChan {
		results[result.pos] = result
		resultCount--
		if resultCount == 0 {
			close(resultChan)
			break
		}
	}

	for _, result := range results {
		if !result.empty {
			answer.highlowcontainer.appendContainer(result.key, result.container, false)
		}
	}

	return answer
}

func TestParAnd(t *testing.T) {
	left := BitmapOf(1, 2)
	right := BitmapOf(1)
	result := GlobalParAggregator.And(left, right)

	if !result.Equals(right) {
		t.Errorf("Result bitmap differs from expected: %v != %v", left, right)
	}
}

func BenchmarkIntersectionSparseParallel(b *testing.B) {
	b.StopTimer()
	initsize := 650000
	r := rand.New(rand.NewSource(0))

	s1 := NewBitmap()
	sz := 150 * 1000 * 1000
	for i := 0; i < initsize; i++ {
		s1.Add(uint32(r.Int31n(int32(sz))))
	}

	s2 := NewBitmap()
	sz = 100 * 1000 * 1000
	for i := 0; i < initsize; i++ {
		s2.Add(uint32(r.Int31n(int32(sz))))
	}

	b.StartTimer()
	card := uint64(0)
	for j := 0; j < b.N; j++ {
		s3 := GlobalParAggregator.And(s1, s2)
		card = card + s3.GetCardinality()
	}
}

func BenchmarkIntersectionSparseRoaring(b *testing.B) {
	b.StopTimer()
	initsize := 650000
	r := rand.New(rand.NewSource(0))

	s1 := NewBitmap()
	sz := 150 * 1000 * 1000
	for i := 0; i < initsize; i++ {
		s1.Add(uint32(r.Int31n(int32(sz))))
	}

	s2 := NewBitmap()
	sz = 100 * 1000 * 1000
	for i := 0; i < initsize; i++ {
		s2.Add(uint32(r.Int31n(int32(sz))))
	}

	b.StartTimer()
	card := uint64(0)
	for j := 0; j < b.N; j++ {
		s3 := And(s1, s2)
		card = card + s3.GetCardinality()
	}
}
