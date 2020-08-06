package timewheel

import (
	"testing"
	"time"
)

func TestTimeWheel(t *testing.T) {
	tw := NewLeveled(2, 2, time.Second)

	tw.Start()

	ch := make(chan string)
	expected := "hello"
	task := &Task{
		ID: "test",
		Cmd: func() {
			ch <- expected
		},
		Delay: time.Second * 5,
	}
	tw.Add(task)

	select {
	case <-time.After(time.Second * 7):
		t.Errorf("got no data")
	case got := <-ch:
		if got != expected {
			t.Errorf("expected:%s,got:%s", expected, got)

		}
	}

}
