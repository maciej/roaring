package roaring

import "testing"

// Verify if bitmap has expected number of containers
// Used for checking test setup, fails test if count is incorrect
func checkContainerCount(t *testing.T, b *Bitmap, expectedContainers int) {
	actualContainers := b.Stats().Containers
	if int(actualContainers) != expectedContainers {
		t.Fatalf("Expected containers %d, actual %d\n", expectedContainers, actualContainers)
	}
}

func TestDetermineOutputContainerCountIntersection(t *testing.T) {
	x1 := BitmapOf(1, 2)
	x2 := BitmapOf(1, 2)

	if determineOutputContainerCount(x1, x2, parOpAnd) != 1 {
		t.Error()
	}

	x2 = BitmapOf(1, 2+(1<<16))
	checkContainerCount(t, x2, 2)

	if determineOutputContainerCount(x1, x2, parOpAnd) != 1 {
		t.Error()
	}

	x1 = BitmapOf(1, 2, 2+2*(1<<16))
	checkContainerCount(t, x1, 2)

	if determineOutputContainerCount(x1, x2, parOpAnd) != 1 {
		t.Error()
	}

	x1 = BitmapOf(1, 2+1<<16)
	x2 = BitmapOf(1, 10+1<<16)
	checkContainerCount(t, x1, 2)
	checkContainerCount(t, x2, 2)
	if determineOutputContainerCount(x1, x2, parOpAnd) != 2 {
		t.Error()
	}

}
