package uuid

import "testing"

func TestGenUniqueJobID(t *testing.T) {
	expectedDelaySecond := uint32(2222)
	expectedPriority := uint8(33)
	id := GenUniqueJobID(expectedDelaySecond, expectedPriority)

	gotDelaySecond, err:= ExtractDelaySecondFromUniqueID(id)
	if err != nil {
		t.Fatal("Failed to extract the delay second")
	}
	if gotDelaySecond != expectedDelaySecond {
		t.Fatalf("Delay second %d was expected, but got %d", expectedDelaySecond, gotDelaySecond)
	}

	gotPriority, err:= ExtractPriorityFromUniqueID(id)
	if err != nil {
		t.Fatal("Failed to extract the priority")
	}
	if gotPriority!= expectedPriority{
		t.Fatalf("Priority %d was expected, but got %d", expectedPriority, gotPriority)
	}
}
