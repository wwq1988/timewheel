package timewheel

import (
	"container/list"
	"sync"
	"time"
)

// TimeWheel TimeWheel
type TimeWheel struct {
	slots    []*list.List
	interval time.Duration
	total    time.Duration
	slot     int64
	pos      int64
	child    *TimeWheel
	parent   *TimeWheel
	level    int64
	addCh    chan *Task
	delCh    chan string
	doneCh   chan struct{}
	id2Pos   map[string]int64
	wg       *sync.WaitGroup
	executor Executor
	sync.RWMutex
}

// Task Task
type Task struct {
	Delay     time.Duration
	ID        string
	Cmd       func()
	circle    int64
	remainder time.Duration
	reAdd     bool
}

// Option Option
type Option func(*TimeWheel)

// Options Options

// WithExecutor WithExecutor
func WithExecutor(executor Executor) Option {
	return func(tw *TimeWheel) {
		tw.executor = executor
	}
}

// New New
func New(slot int64, interval time.Duration, opts ...Option) *TimeWheel {
	return NewLeveled(1, slot, interval, opts...)
}

// NewLeveled NewLeveled
func NewLeveled(level, slot int64, interval time.Duration, opts ...Option) *TimeWheel {
	var prev *TimeWheel
	var tw *TimeWheel
	wg := &sync.WaitGroup{}
	for curLevel := int64(0); curLevel < level; curLevel++ {
		var cur *TimeWheel
		if prev == nil {
			cur = newTimeWheel(slot, interval, opts...)
		} else {
			cur = newTimeWheel(slot, prev.total, opts...)
		}
		cur.parent = prev
		cur.level = curLevel
		cur.wg = wg
		if prev != nil {
			prev.child = cur
		}
		if curLevel == 0 {
			tw = cur
		}
		prev = cur
	}

	return tw
}

func newTimeWheel(slot int64, interval time.Duration, opts ...Option) *TimeWheel {
	ltw := &TimeWheel{
		slots:    make([]*list.List, slot),
		interval: interval,
		slot:     slot,
		addCh:    make(chan *Task),
		delCh:    make(chan string),
		doneCh:   make(chan struct{}),
		total:    time.Duration(slot) * interval,
		id2Pos:   make(map[string]int64),
		executor: &defaultExecutor{},
	}
	for _, opt := range opts {
		opt(ltw)
	}

	for i := int64(0); i < slot; i++ {
		ltw.slots[i] = list.New()
	}
	return ltw
}

// Start Start
func (tw *TimeWheel) Start() {
	child := tw.child
	if child != nil {
		child.Start()
	}

	go tw.start()
}

func (tw *TimeWheel) start() {
	tw.wg.Add(1)
	ticker := time.NewTicker(tw.interval)

	defer func() {
		tw.wg.Done()
		ticker.Stop()
	}()
	for {
		select {
		case <-ticker.C:
			tw.tickerHandler()
		case task := <-tw.addCh:
			tw.addHandler(task)
		case id := <-tw.delCh:
			tw.delHandler(id)
		case <-tw.doneCh:
			return
		}
	}
}
func (tw *TimeWheel) tickerHandler() {

	if tw.pos == tw.slot-1 {
		tw.pos = 0
	} else {
		tw.pos++
	}

	slot := tw.slots[tw.pos]
	for e := slot.Front(); e != nil; e = e.Next() {
		task := e.Value.(*Task)
		if task.circle > 0 {
			task.circle--
			continue
		}

		if task.remainder != 0 && tw.parent != nil {
			task.reAdd = true
			tw.parent.Add(task)
			continue
		}
		tw.executor.Exec(task.Cmd)
		slot.Remove(e)
	}
}

func (tw *TimeWheel) getPostAndCircleAndRemainder(delay time.Duration) (int64, int64, time.Duration) {
	delaySeconds := int64(delay.Seconds())
	intervalSeconds := int64(tw.interval.Seconds())
	circle := delaySeconds / (intervalSeconds * tw.slot)

	remainderAll := delaySeconds - tw.slot*circle*intervalSeconds
	pos := remainderAll / intervalSeconds
	remainder := remainderAll % intervalSeconds
	return pos, circle, time.Duration(remainder) * time.Second
}

func (tw *TimeWheel) addHandler(task *Task) {
	delay := task.Delay
	if task.reAdd {
		delay = task.remainder
	}
	if delay < tw.interval && tw.parent != nil {
		tw.parent.Add(task)
		return
	}

	if delay > tw.total && tw.child != nil {
		go tw.child.Add(task)
		return
	}
	pos, circle, remainder := tw.getPostAndCircleAndRemainder(delay)
	task.circle = circle

	task.remainder = remainder
	tw.slots[pos].PushBack(task)
	tw.id2Pos[task.ID] = pos

}
func (tw *TimeWheel) delHandler(id string) {
	if tw.parent != nil {
		tw.parent.delCh <- id
	}
	pos, exist := tw.id2Pos[id]
	if !exist {
		return
	}
	slot := tw.slots[pos]
	var found bool
	for e := slot.Front(); e != nil; e = e.Next() {
		task := e.Value.(*Task)
		if task.ID == id {
			slot.Remove(e)
			found = true
		}
	}
	if !found {
		go tw.child.Del(id)
	}
}

// Add Add
func (tw *TimeWheel) Add(task *Task) {
	tw.addCh <- task
}

// Del Del
func (tw *TimeWheel) Del(id string) {
	tw.delCh <- id
}

// Stop Stop
func (tw *TimeWheel) Stop() {
	child := tw.child
	if child != nil {
		child.Stop()
	}
	close(tw.doneCh)
}

// GracefulStop GracefulStop
func (tw *TimeWheel) GracefulStop() {
	child := tw.child
	if child != nil {
		child.Stop()
	}
	close(tw.doneCh)
	tw.wg.Wait()
}
