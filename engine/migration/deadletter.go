package migration

import "github.com/bitleak/lmstfy/engine"

type DeadLetter struct {
	meta engine.QueueMeta
	e    *Engine
}

func (dl DeadLetter) Peek() (size int64, jobID string, err error) {
	size1, jobID1, err := dl.e.oldEngine.DeadLetter(dl.meta).Peek()
	if err != nil && err != engine.ErrNotFound {
		return 0, "", err
	}
	size2, jobID2, err := dl.e.newEngine.DeadLetter(dl.meta).Peek()
	if err != nil {
		return 0, "", err
	}
	if size1 == 0 {
		return size2, jobID2, nil
	} else {
		return size1 + size2, jobID1, nil // If both engines has deadletter, return the old engine's job ID
	}
}
func (dl DeadLetter) Delete(limit int64) (count int64, err error) {
	count1, err := dl.e.oldEngine.DeadLetter(dl.meta).Delete(limit)
	if err != nil {
		return 0, err
	}
	count2, err := dl.e.newEngine.DeadLetter(dl.meta).Delete(limit - count1)
	return count1 + count2, err
}

func (dl DeadLetter) Respawn(limit, ttlSecond int64) (count int64, err error) {
	count1, err := dl.e.oldEngine.DeadLetter(dl.meta).Respawn(limit, ttlSecond)
	if err != nil {
		return 0, err
	}
	count2, err := dl.e.newEngine.DeadLetter(dl.meta).Respawn(limit-count1, ttlSecond)
	return count1 + count2, err
}

func (dl DeadLetter) Size() (size int64, err error) {
	size1, err := dl.e.oldEngine.DeadLetter(dl.meta).Size()
	if err != nil {
		return 0, err
	}
	size2, err := dl.e.newEngine.DeadLetter(dl.meta).Size()
	return size1 + size2, err
}
