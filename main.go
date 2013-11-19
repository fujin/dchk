package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const (
	numPollers     = 1
	pollInterval   = 60 * time.Second
	statusInterval = 1 * time.Minute
	errTimeout     = 10 * time.Second
)

var (
	threshold = flag.Uint64("threshold", uint64(1.074*10E11), "Threshold to start serving 500's over HTTP")
	addr      = flag.String("addr", ":8080", "Listen address for HTTP")
	path      = flag.String("path", "/mnt/storage", "The path to query for disk usage")
)

// State represents the last-known state of a path
type State struct {
	path  string
	bytes uint64
}

// StateMonitor maintains a map that stores the state of the URLs
// being
// polled, and prints the current state every updateInterval
// nanoseconds.
// It returns a chan State to which resource state should be sent.
func StateMonitor(updateInterval time.Duration) chan<- State {
	updates := make(chan State)
	diskStatus := make(map[string]uint64)
	ticker := time.NewTicker(updateInterval)
	go func() {
		for {
			select {
			case <-ticker.C:
				logState(diskStatus)
			case s := <-updates:
				diskStatus[s.path] = s.bytes
			}
		}
	}()
	go func() {
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			bytes := diskStatus[*path]
			fmt.Fprintln(w, bytes)
		})
		if err := http.ListenAndServe(*addr, nil); err != nil {
			log.Fatal("ListenAndServe failed: ", err)
		}
	}()
	return updates
}

// logState prints a state map.
func logState(s map[string]uint64) {
	log.Println("Current state:")
	for k, v := range s {
		log.Printf(" %s %v", k, v)
	}
}

// Path represents a filesystem directory to be polled with du
type Path struct {
	path     string
	errCount int
}

// Poll executes du for a path
// and returns the disk usage in bytes or an error string
func (r *Path) Poll() (bytes uint64) {
	out, err := exec.Command("du", "-sbx", r.path).Output()
	if err != nil {
		log.Fatal(err)
		r.errCount++
	}
	// Tidy up the line
	s := string(out)
	s = strings.TrimSpace(s)

	// Parse tabulation
	bytes_str := strings.Split(s, "\t")[0]

	// Parse uint64 from string
	bytes, err = strconv.ParseUint(bytes_str, 0, 64)
	if err != nil {
		log.Fatal(err)
		r.errCount++
	}
	r.errCount = 0
	return bytes
}

// Sleep sleeps for an appropriate interval (dependent on error state)
// before sending the Path to done.
func (r *Path) Sleep(done chan<- *Path) {
	time.Sleep(pollInterval + errTimeout*time.Duration(r.errCount))
	done <- r
}

func Poller(in <-chan *Path, out chan<- *Path, status chan<- State) {
	for r := range in {
		bytes := r.Poll()
		status <- State{r.path, bytes}
		out <- r
	}
}

func main() {
	// Create our input and output channels.
	pending, complete := make(chan *Path), make(chan *Path)

	// Launch the StateMonitor.
	status := StateMonitor(statusInterval)
	log.Println("State Monitor started")

	// Launch some Poller goroutines.
	for i := 0; i < numPollers; i++ {
		go Poller(pending, complete, status)
	}

	// Send the path flag to the pending queue
	go func() {
		pending <- &Path{path: *path}
	}()

	for r := range complete {
		go r.Sleep(pending)
	}

}
