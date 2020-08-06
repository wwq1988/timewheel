package timewheel

import "sync"

// Executor Executor
type Executor interface {
	Exec(task func())
}

type defaultExecutor struct{}

func (de *defaultExecutor) Exec(task func()) {
	task()
}

// NewDefaultExecutor NewDefaultExecutor
func NewDefaultExecutor() Executor {
	return &defaultExecutor{}
}

type pooledExecutor struct {
	taskCh chan func()
	doneCh chan struct{}
	wg     *sync.WaitGroup
}

// NewPooledExecutor NewPooledExecutor
func NewPooledExecutor(size int) Executor {
	pe := &pooledExecutor{
		taskCh: make(chan func()),
		doneCh: make(chan struct{}),
		wg:     &sync.WaitGroup{},
	}
	for i := 0; i < size; i++ {
		pe.wg.Add(1)
		pe.loop()
	}
	return pe
}

func (pe *pooledExecutor) loop() {
	defer pe.wg.Done()
	for {
		select {
		case task := <-pe.taskCh:
			task()
		case <-pe.doneCh:
			return
		}
	}
}

func (pe *pooledExecutor) Close() {
	close(pe.doneCh)
	pe.wg.Wait()
}

func (pe *pooledExecutor) Exec(task func()) {
	pe.taskCh <- task
}
