package roaring

import "runtime"

type parOp int

const (
	parOpAnd    parOp = iota
	parOpOr
	parOpXor
	parOpAndNot
)

var defaultWorkerCount int = runtime.NumCPU()

const defaultTaskQueueLength = 4096

type ParAggregator struct {
	taskQueue chan parTask
}

func NewParAggregator(taskQueueLength, workerCount int) ParAggregator {
	agg := ParAggregator{
		make(chan parTask, taskQueueLength),
	}

	for i := 0; i < workerCount; i++ {
		go agg.worker()
	}

	return agg
}

func NewParAggregatorWithDefaults() ParAggregator {
	return NewParAggregator(defaultTaskQueueLength, defaultWorkerCount)
}

func (aggregator ParAggregator) worker() {
	for task := range aggregator.taskQueue {
		var resultContainer container
		switch task.op {
		case parOpAnd:
			resultContainer = task.left.and(task.right)
		}

		result := parContainer{
			key:       task.key,
			idx:       task.idx,
			container: resultContainer,
		}

		task.result <- result
	}
}

func (aggregator ParAggregator) Shutdown() {
	close(aggregator.taskQueue)
}

type parTask struct {
	op          parOp
	key         uint16
	idx         int
	left, right container
	result      chan<- parContainer
}

type parContainer struct {
	key       uint16
	idx       int
	container container
}

// TODO add tests
func determineOutputContainerCount(x1, x2 *Bitmap, operation parOp) uint16 {
	count := uint16(0)
	pos1 := 0
	pos2 := 0
	length1 := x1.highlowcontainer.size()
	length2 := x2.highlowcontainer.size()

	switch operation {
	case parOpAnd:
		for pos1 < length1 && pos2 < length2 {
			key1 := x1.highlowcontainer.getKeyAtIndex(pos1)
			key2 := x2.highlowcontainer.getKeyAtIndex(pos2)

			if key1 == key2 {
				pos1++
				pos2++
				count++
			} else if key1 < key2 {
				pos1 = x1.highlowcontainer.advanceUntil(key2, pos1)
			} else {
				pos2 = x2.highlowcontainer.advanceUntil(key1, pos2)
			}
		}
	case parOpAndNot:
		for pos1 < length1 && pos2 < length2 {
			key1 := x1.highlowcontainer.getKeyAtIndex(pos1)
			key2 := x2.highlowcontainer.getKeyAtIndex(pos2)

			if key1 == key2 {
				pos1++
				pos2++
				count++
			} else if key1 < key2 {
				pos1 = x1.highlowcontainer.advanceUntil(key2, pos1)
			} else {
				pos2 = x2.highlowcontainer.advanceUntil(key1, pos2)
				count++
			}
		}
		count += uint16(length1)
	default:
		for pos1 < length1 && pos2 < length2 {
			key1 := x1.highlowcontainer.getKeyAtIndex(pos1)
			key2 := x2.highlowcontainer.getKeyAtIndex(pos2)

			if key1 == key2 {
				pos1++
				pos2++
				count++
			} else if key1 < key2 {
				count++
				pos1++
			} else {
				count++
				pos2++
			}
		}
		count += uint16((length2 - pos2) + (length1 - pos1))
	}

	return count
}

//func (aggregator ParAggregator) walkBitmapPair()

//func receiveContainers(containerChan <-chan parContainer, containerCount uint16) chan *Bitmap {
//
//}

func (aggregator ParAggregator) And(x1, x2 *Bitmap) *Bitmap {

	pos1 := 0
	pos2 := 0
	length1 := x1.highlowcontainer.size()
	length2 := x2.highlowcontainer.size()

	containerIdx := 0
	containerCount := determineOutputContainerCount(x1, x2, parOpAnd)
	containerChan := make(chan parContainer, containerCount)

main:
	for pos1 < length1 && pos2 < length2 {
		s1 := x1.highlowcontainer.getKeyAtIndex(pos1)
		s2 := x2.highlowcontainer.getKeyAtIndex(pos2)
		for {
			if s1 == s2 {
				left := x1.highlowcontainer.getContainerAtIndex(pos1)
				right := x2.highlowcontainer.getContainerAtIndex(pos2)

				aggregator.taskQueue <- parTask{
					op:     parOpAnd,
					left:   left,
					right:  right,
					key:    s1,
					idx:    containerIdx,
					result: containerChan,
				}

				containerIdx++
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

	// TODO receive containers

	return NewBitmap()
}
