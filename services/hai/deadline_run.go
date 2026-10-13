//go:build linux

// The in-image deadline entrypoint of a finite job in every finite-job image (installed as /opt/hai/deadline_run).
//
//	HAI_DEADLINE_UTC=<epoch seconds> /opt/hai/deadline_run <command> [argument ...]
//
// It keeps the contract of deadline_run.py (D1 section 5 item 1) and uses only the Go standard library; it is built static
// (CGO_ENABLED=0) by services/lakehouse/polaris_admin/Dockerfile.
//
//  1. A bracketed sample: monotonic M1, wall W0, monotonic M2. A bracket wider than 50 ms is retried up to 10 times and then
//     the program exits 124 without starting the command. E = M1 + (D - S - W0) is the single absolute monotonic expiry;
//     S (2 s) is the admitted host-to-VM clock skew. The monotonic clock is CLOCK_MONOTONIC, which every process shares.
//  2. B = E - S - 1 is the command-start boundary: the command never starts when the monotonic clock is not before B.
//  3. The program starts itself again in a new process group (the relay). The relay rechecks that the monotonic clock is before
//     B immediately before it replaces itself with the command by exec, and exits 124 without running the command otherwise.
//     At E the group gets TERM; at E + K (K = 55 s) it gets KILL; the program then exits 124. Only ESRCH proves a group ended:
//     a denied signal (EPERM) is bounded by a 5 s absence-proof wait and reported on stderr with exit code 123.
//
// Exit codes: the command's own code (128 + signal when it was killed); 124 on a missed boundary or a deadline stop; 125 when the
// deadline variable or the command is missing or malformed; 127 when the command is not found; 123 when a group is not proved ended.
package main

import (
	"fmt"
	"math"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"time"
	"unsafe"
)

const (
	skew           = 2.0
	killGrace      = 55.0
	bracketLimit   = 0.050
	bracketTries   = 10
	startMargin    = 1.0
	poll           = 20 * time.Millisecond
	reapWait       = 5.0
	timeoutExit    = 124
	usageExit      = 125
	unconfirmed    = 123
	notFoundExit   = 127
	relayVariable  = "HAI_DEADLINE_RELAY_B"
	clockMonotonic = 1
)

// mono returns CLOCK_MONOTONIC in seconds; every process of the container reads the same clock.
func clock(clockID uintptr) float64 {
	var ts syscall.Timespec
	if _, _, errno := syscall.Syscall(syscall.SYS_CLOCK_GETTIME, clockID, uintptr(unsafe.Pointer(&ts)), 0); errno != 0 {
		fmt.Fprintf(os.Stderr, "deadline_run: clock_gettime failed: %v\n", errno)
		os.Exit(usageExit)
	}
	return float64(ts.Sec) + float64(ts.Nsec)/1e9
}

func mono() float64 { return clock(clockMonotonic) }
func wall() float64 { return clock(0) }

// bracketedSample returns (M1, W0) from a sample whose monotonic bracket is at most 50 ms.
func bracketedSample() (float64, float64, bool) {
	for i := 0; i < bracketTries; i++ {
		first := mono()
		wallNow := wall()
		second := mono()
		if second-first <= bracketLimit {
			return first, wallNow, true
		}
	}
	return 0, 0, false
}

func sleep(seconds float64) {
	time.Sleep(time.Duration(seconds * float64(time.Second)))
}

// groupState probes a process group with signal 0: "gone" (ESRCH proves it ended), "alive" or "denied" (EPERM).
func groupState(group int) string {
	switch err := syscall.Kill(-group, 0); err {
	case nil:
		return "alive"
	case syscall.ESRCH:
		return "gone"
	case syscall.EPERM:
		return "denied"
	default:
		return "alive"
	}
}

// signalGroup sends a signal to the group and returns "sent", "gone" (ESRCH) or "denied" (EPERM).
func signalGroup(group int, number syscall.Signal) string {
	switch err := syscall.Kill(-group, number); err {
	case nil:
		return "sent"
	case syscall.ESRCH:
		return "gone"
	case syscall.EPERM:
		return "denied"
	default:
		return "sent"
	}
}

func waitGroupGone(group int, until float64) bool {
	for {
		if groupState(group) == "gone" {
			return true
		}
		if mono() >= until {
			return false
		}
		time.Sleep(poll)
	}
}

func exitCode(status syscall.WaitStatus) int {
	if status.Signaled() {
		return 128 + int(status.Signal())
	}
	return status.ExitStatus()
}

func notProved(group int, what string) int {
	fmt.Fprintf(os.Stderr, "deadline_run: process group %d not proved ended after %s; the launcher removes the container\n", group, what)
	return unconfirmed
}

// relay runs in the new process group: recheck B immediately before exec, then become the command.
func relay(command []string, boundary float64) int {
	if mono() >= boundary {
		return timeoutExit
	}
	path, err := exec.LookPath(command[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "deadline_run: %s not found\n", command[0])
		return notFoundExit
	}
	environment := make([]string, 0, len(os.Environ()))
	for _, entry := range os.Environ() {
		if len(entry) < len(relayVariable)+1 || entry[:len(relayVariable)+1] != relayVariable+"=" {
			environment = append(environment, entry)
		}
	}
	if mono() >= boundary {
		return timeoutExit
	}
	err = syscall.Exec(path, command, environment)
	fmt.Fprintf(os.Stderr, "deadline_run: exec %s failed: %v\n", command[0], err)
	return usageExit
}

func run(command []string, deadline float64) int {
	first, wallNow, ok := bracketedSample()
	if !ok {
		return timeoutExit
	}
	stopAt := first + (deadline - skew - wallNow)
	boundary := stopAt - skew - startMargin
	if mono() >= boundary {
		return timeoutExit
	}
	self, err := os.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "deadline_run: cannot find itself: %v\n", err)
		return usageExit
	}
	environment := append(os.Environ(), relayVariable+"="+strconv.FormatFloat(boundary, 'f', -1, 64))
	forwarded := make(chan os.Signal, 4)
	signal.Notify(forwarded, syscall.SIGTERM, syscall.SIGINT)
	pid, err := syscall.ForkExec(self, append([]string{self}, command...), &syscall.ProcAttr{
		Env:   environment,
		Files: []uintptr{0, 1, 2},
		Sys:   &syscall.SysProcAttr{Setpgid: true},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "deadline_run: cannot start the command: %v\n", err)
		return usageExit
	}
	var status syscall.WaitStatus
	termed, killed := false, false
	for {
		select {
		case <-forwarded:
			signalGroup(pid, syscall.SIGTERM)
		default:
		}
		done, err := syscall.Wait4(pid, &status, syscall.WNOHANG, nil)
		if err == nil && done == pid {
			break
		}
		now := mono()
		if !termed && now >= stopAt {
			signalGroup(pid, syscall.SIGTERM)
			termed = true
		}
		if !killed && now >= stopAt+killGrace {
			signalGroup(pid, syscall.SIGKILL)
			killed = true
			limit := stopAt + killGrace + reapWait
			reaped := false
			for mono() < limit {
				done, err := syscall.Wait4(pid, &status, syscall.WNOHANG, nil)
				if err == nil && done == pid {
					reaped = true
					break
				}
				time.Sleep(poll)
			}
			if !reaped {
				return notProved(pid, "KILL (the group leader was not reaped)")
			}
			break
		}
		time.Sleep(poll)
	}
	if outcome := signalGroup(pid, syscall.SIGKILL); outcome != "gone" && !waitGroupGone(pid, mono()+reapWait) {
		return notProved(pid, "the final KILL")
	}
	if termed {
		return timeoutExit
	}
	return exitCode(status)
}

func main() {
	if value, relaying := os.LookupEnv(relayVariable); relaying {
		boundary, err := strconv.ParseFloat(value, 64)
		if err != nil || math.IsNaN(boundary) || math.IsInf(boundary, 0) || len(os.Args) < 2 {
			os.Exit(usageExit)
		}
		os.Exit(relay(os.Args[1:], boundary))
	}
	command := os.Args[1:]
	if len(command) > 0 && command[0] == "--" {
		command = command[1:]
	}
	deadline, err := strconv.ParseFloat(os.Getenv("HAI_DEADLINE_UTC"), 64)
	if err != nil || math.IsNaN(deadline) || math.IsInf(deadline, 0) {
		fmt.Fprintln(os.Stderr, "deadline_run: HAI_DEADLINE_UTC must be epoch seconds")
		os.Exit(usageExit)
	}
	if len(command) == 0 {
		fmt.Fprintln(os.Stderr, "deadline_run: no command")
		os.Exit(usageExit)
	}
	os.Exit(run(command, deadline))
}
