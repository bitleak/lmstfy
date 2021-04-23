package migration

import "github.com/bitleak/lmstfy/engine"

type DeadLetter struct {
	meta engine.QueueMeta
	e    *Engine
}

func (dl DeadLetter) Peek() (size int64, jobID string, err error) {
	oldDl, err := dl.e.oldEngine.DeadLetter(dl.meta)
	if err != nil {
		return 0, "", err
	}
	size1, jobID1, err := oldDl.Peek()
	if err != nil && err != engine.ErrNotFound {
		return 0, "", err
	}

	newDl, err := dl.e.newEngine.DeadLetter(dl.meta)
	if err != nil {
		return 0, "", err
	}
	size2, jobID2, err := newDl.Peek()
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
	oldDl, err := dl.e.oldEngine.DeadLetter(dl.meta)
	if err != nil {
		return 0, err
	}
	count1, err := oldDl.Delete(limit)
	if err != nil {
		return 0, err
	}

	newDl, err := dl.e.newEngine.DeadLetter(dl.meta)
	if err != nil {
		return count1, err
	}
	count2, err := newDl.Delete(limit - count1)
	return count1 + count2, err
}

func (dl DeadLetter) Respawn(limit, ttlSecond int64) (count int64, err error) {
	oldDl, err := dl.e.oldEngine.DeadLetter(dl.meta)
	if err != nil {
		return 0, err
	}
	count1, err := oldDl.Respawn(limit, ttlSecond)
	if err != nil {
		return 0, err
	}

	newDl, err := dl.e.newEngine.DeadLetter(dl.meta)
	if err != nil {
		return count1, err
	}
	count2, err := newDl.Respawn(limit-count1, ttlSecond)
	return count1 + count2, err
}

func (dl DeadLetter) Size() (size int64, err error) {
	oldDl, err := dl.e.oldEngine.DeadLetter(dl.meta)
	if err != nil {
		return 0, err
	}
	size1, err := oldDl.Size()
	if err != nil {
		return 0, err
	}

	newDl, err := dl.e.newEngine.DeadLetter(dl.meta)
	if err != nil {
		return size1, err
	}
	size2, err := newDl.Size()
	return size1 + size2, err
}
