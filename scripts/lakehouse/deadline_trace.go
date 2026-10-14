//go:build linux

// A standalone end-to-end fault verifier of an installed finite-job deadline binary.
// It records the real exec event and every traced group process, without modifying the tested binary.
package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unsafe"
)

type Scenario struct {
	Name                       string   `json:"name"`
	DeadlineIn                 float64  `json:"deadline_in"`
	WallOffset                 float64  `json:"wall_offset"`
	Delay                      float64  `json:"start_delay"`
	Stall                      string   `json:"stall"`
	BoundaryOffset             *float64 `json:"boundary_offset"`
	Stubborn                   bool     `json:"stubborn"`
	DenyTerm                   bool     `json:"deny_term"`
	DenyKill                   bool     `json:"deny_kill"`
	TransientProbe             bool     `json:"transient_probe"`
	WideBrackets               bool     `json:"wide_brackets"`
	Load                       bool     `json:"load"`
	WantExit                   int      `json:"want_exit"`
	WantExec                   bool     `json:"want_exec"`
	Command                    []string `json:"command"`
	MissingDeadline            bool     `json:"missing_deadline"`
	NoCommand                  bool     `json:"no_command"`
	LoadWorkers                int      `json:"load_workers"`
	ExpectedRequirementFailure bool     `json:"expected_requirement_failure"`
	DeadlineText               string   `json:"deadline_text"`
}

type Stamp struct {
	Kind string  `json:"kind"`
	Host float64 `json:"host"`
	PID  int     `json:"pid"`
}
type Result struct {
	Name           string  `json:"name"`
	Binary         string  `json:"binary_sha256"`
	Deadline       float64 `json:"deadline"`
	Cutoff         float64 `json:"cutoff"`
	Exit           int     `json:"exit"`
	Exec           float64 `json:"exec"`
	LastGroupExit  float64 `json:"last_group_exit"`
	Signals        []Stamp `json:"signals"`
	Faults         []Stamp `json:"faults"`
	Error          string  `json:"error"`
	Pass           bool    `json:"pass"`
	HarnessCleanup bool    `json:"harness_cleanup"`
	CleanupDone    bool    `json:"cleanup_done"`
	RequirementMet bool    `json:"requirement_met"`
}

type Call struct {
	Number  uint64
	A0      uint64
	A1      uint64
	Denied  bool
	Clock   bool
	ClockID uint64
	Pointer uintptr
}

func host() float64 { return float64(time.Now().UnixNano()) / 1e9 }
func pause(seconds float64) {
	if seconds > 0 {
		time.Sleep(time.Duration(seconds * float64(time.Second)))
	}
}
func field(regs *syscall.PtraceRegs, name string) reflect.Value {
	return reflect.ValueOf(regs).Elem().FieldByName(name)
}
func reg(regs *syscall.PtraceRegs, index int) uint64 {
	if runtime.GOARCH == "arm64" {
		return field(regs, "Regs").Index(index).Uint()
	}
	names := []string{"Rdi", "Rsi", "Rdx"}
	return field(regs, names[index]).Uint()
}
func setreg(regs *syscall.PtraceRegs, index int, value uint64) {
	if runtime.GOARCH == "arm64" {
		field(regs, "Regs").Index(index).SetUint(value)
		return
	}
	names := []string{"Rdi", "Rsi", "Rdx"}
	field(regs, names[index]).SetUint(value)
}
func number(regs *syscall.PtraceRegs) uint64 {
	if runtime.GOARCH == "arm64" {
		return field(regs, "Regs").Index(8).Uint()
	}
	return field(regs, "Orig_rax").Uint()
}
func retval(regs *syscall.PtraceRegs) int64 {
	if runtime.GOARCH == "arm64" {
		return int64(field(regs, "Regs").Index(0).Uint())
	}
	return int64(field(regs, "Rax").Uint())
}
func setreturn(regs *syscall.PtraceRegs, value int64) {
	if runtime.GOARCH == "arm64" {
		field(regs, "Regs").Index(0).SetUint(uint64(value))
		return
	}
	field(regs, "Rax").SetUint(uint64(value))
}
func tgid(pid int) int {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return pid
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "Tgid:") {
			v, _ := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "Tgid:")))
			return v
		}
	}
	return pid
}
func patchClock(pid int, ptr uintptr, seconds float64) error {
	raw := make([]byte, 16)
	if _, err := syscall.PtracePeekData(pid, ptr, raw); err != nil {
		return err
	}
	sec := int64(binary.LittleEndian.Uint64(raw[:8]))
	ns := int64(binary.LittleEndian.Uint64(raw[8:]))
	ns += int64(seconds * 1e9)
	for ns >= 1e9 {
		sec++
		ns -= 1e9
	}
	for ns < 0 {
		sec--
		ns += 1e9
	}
	binary.LittleEndian.PutUint64(raw[:8], uint64(sec))
	binary.LittleEndian.PutUint64(raw[8:], uint64(ns))
	_, err := syscall.PtracePokeData(pid, ptr, raw)
	return err
}

func livingGroup(live, groups map[int]bool) bool {
	for pid := range live {
		if !groups[pid] {
			continue
		}
		raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
		if err == nil && !strings.Contains(string(raw), "State:\tZ") && !strings.Contains(string(raw), "State:\tX") {
			return true
		}
	}
	return false
}

// vanished reports a ptrace error on a tracee that was killed or reaped between its stop and this call; its exit arrives through
// wait4, so the tracer skips the stop instead of failing the scenario.
func vanished(err error) bool { return errors.Is(err, syscall.ESRCH) }

func syscallStop(pid int) (byte, error) {
	var raw [88]byte
	_, _, errno := syscall.Syscall6(syscall.SYS_PTRACE, 0x420e, uintptr(pid), uintptr(len(raw)), uintptr(unsafe.Pointer(&raw[0])), 0, 0)
	if errno != 0 {
		return 0, errno
	}
	return raw[0], nil
}

func verify(binaryPath string, s Scenario) (r Result) {
	raw, err := os.ReadFile(binaryPath)
	if err != nil {
		r.Error = err.Error()
		return
	}
	r.Binary = fmt.Sprintf("%x", sha256.Sum256(raw))
	r.Name = s.Name
	r.Deadline = host() + s.DeadlineIn
	r.Cutoff = r.Deadline - 3
	r.Exec = 0
	r.Exit = -1
	pause(s.Delay)
	loads := []*exec.Cmd{}
	if s.Load {
		workers := s.LoadWorkers
		if workers < 1 {
			workers = 1
		}
		for i := 0; i < workers; i++ {
			load := exec.Command("/bin/sh", "-c", "while :; do :; done")
			if err = load.Start(); err != nil {
				r.Error = err.Error()
				return
			}
			loads = append(loads, load)
		}
		defer func() {
			for _, load := range loads {
				_ = load.Process.Kill()
				_ = load.Wait()
			}
		}()
	}
	script := "exit 7"
	if s.Stubborn {
		script = "trap '' TERM; sleep 1000 & wait"
	}
	command := []string{"/bin/sh", "-c", script}
	if s.Command != nil {
		command = s.Command
	}
	if s.NoCommand {
		command = []string{}
	}
	cmd := exec.Command(binaryPath, command...)
	cmd.Env = append(os.Environ(), "GOMAXPROCS=1")
	if !s.MissingDeadline {
		value := strconv.FormatFloat(r.Deadline, 'f', -1, 64)
		if s.DeadlineText != "" {
			value = s.DeadlineText
		}
		cmd.Env = append(cmd.Env, "HAI_DEADLINE_UTC="+value)
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Ptrace: true}
	if err = cmd.Start(); err != nil {
		r.Error = err.Error()
		return
	}
	root := cmd.Process.Pid
	live := map[int]bool{root: true}
	groups := map[int]bool{}
	calls := map[int]Call{}
	entry := map[int]bool{}
	monos := map[int]int{}
	relays := map[int]bool{}
	group := 0
	stallDone := false
	probeCount := 0
	rootEnded := false
	defer func() {
		if len(live) > 0 {
			for pid := range live {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
			for len(live) > 0 {
				var st syscall.WaitStatus
				pid, e := syscall.Wait4(-1, &st, syscall.WALL, nil)
				if e != nil {
					break
				}
				if st.Exited() || st.Signaled() {
					delete(live, pid)
				} else {
					_ = syscall.PtraceCont(pid, int(syscall.SIGKILL))
				}
			}
		}
		r.CleanupDone = len(live) == 0
		_ = cmd.Process.Release()
	}()
	var st syscall.WaitStatus
	if _, err = syscall.Wait4(root, &st, 0, nil); err != nil {
		r.Error = err.Error()
		return
	}
	options := syscall.PTRACE_O_TRACESYSGOOD | syscall.PTRACE_O_TRACEFORK | syscall.PTRACE_O_TRACEVFORK | syscall.PTRACE_O_TRACECLONE | syscall.PTRACE_O_TRACEEXEC | 0x00100000
	if err = syscall.PtraceSetOptions(root, options); err != nil {
		r.Error = err.Error()
		return
	}
	if err = syscall.PtraceSyscall(root, 0); err != nil {
		r.Error = err.Error()
		return
	}
	limit := r.Deadline + 75
	for len(live) > 0 {
		if host() > limit {
			r.Error = "tracer exceeded its bounded diagnostic limit"
			return
		}
		pid, e := syscall.Wait4(-1, &st, syscall.WALL|syscall.WNOHANG, nil)
		if e == syscall.EINTR {
			continue
		}
		if e == syscall.ECHILD {
			for tracked := range live {
				if _, absent := os.Stat(fmt.Sprintf("/proc/%d/status", tracked)); os.IsNotExist(absent) {
					delete(live, tracked)
				}
			}
			if len(live) == 0 {
				break
			}
		}
		if e != nil {
			r.Error = e.Error()
			return
		}
		if pid == 0 {
			time.Sleep(time.Millisecond)
			continue
		}
		if st.Exited() || st.Signaled() {
			delete(live, pid)
			delete(calls, pid)
			delete(entry, pid)
			if groups[pid] {
				r.LastGroupExit = host()
			}
			if pid == root {
				r.Exit = st.ExitStatus()
				if st.Signaled() {
					r.Exit = 128 + int(st.Signal())
				}
				rootEnded = true
			}
			if rootEnded && group > 0 && !r.HarnessCleanup && livingGroup(live, groups) {
				r.HarnessCleanup = true
				_ = syscall.Kill(-group, syscall.SIGKILL)
			}
			continue
		}
		if !st.Stopped() {
			continue
		}
		live[pid] = true
		sig := st.StopSignal()
		cause := st.TrapCause()
		if sig == syscall.SIGTRAP && cause != 0 {
			if cause == syscall.PTRACE_EVENT_FORK || cause == syscall.PTRACE_EVENT_VFORK || cause == syscall.PTRACE_EVENT_CLONE {
				child, ee := syscall.PtraceGetEventMsg(pid)
				if vanished(ee) {
					continue
				}
				if ee != nil {
					r.Error = ee.Error()
					return
				}
				live[int(child)] = true
				entry[int(child)] = true
				if groups[pid] {
					groups[int(child)] = true
				}
			}
			if cause == syscall.PTRACE_EVENT_EXEC {
				old, oldErr := syscall.PtraceGetEventMsg(pid)
				if oldErr == nil && int(old) != pid {
					delete(live, int(old))
					delete(entry, int(old))
					delete(calls, int(old))
				}
				path, _ := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
				if path == binaryPath && pid != root {
					relays[tgid(pid)] = true
				} else if pid != root && !strings.HasSuffix(path, "/deadline_run") && r.Exec == 0 {
					r.Exec = host()
					group = pid
					groups[pid] = true
				}
			}
			if e = syscall.PtraceSyscall(pid, 0); e != nil && e != syscall.ESRCH {
				r.Error = e.Error()
				return
			}
			continue
		}
		if sig == syscall.Signal(int(syscall.SIGTRAP)|0x80) {
			var regs syscall.PtraceRegs
			if e = syscall.PtraceGetRegs(pid, &regs); vanished(e) {
				continue
			}
			if e != nil {
				r.Error = e.Error()
				return
			}
			stop, stopErr := syscallStop(pid)
			if vanished(stopErr) {
				continue
			}
			if stopErr != nil {
				r.Error = stopErr.Error()
				return
			}
			if stop == 1 {
				entry[pid] = true
				c := Call{Number: number(&regs), A0: reg(&regs, 0), A1: reg(&regs, 1)}
				if c.Number == uint64(syscall.SYS_CLOCK_GETTIME) {
					c.Clock = true
					c.ClockID = c.A0
					c.Pointer = uintptr(c.A1)
					process := tgid(pid)
					if c.ClockID == 1 {
						monos[process]++
						hit := monos[process]
						inside := process == root && hit == 2 && s.Stall == "inside"
						outside := process == root && hit == 3 && s.Stall == "outside"
						watchdog := process == root && s.Stall == "watchdog" && host() >= r.Deadline-2
						last := relays[process] && hit == 2
						if !stallDone && (inside || outside || watchdog || (last && (s.Stall == "relay" || s.BoundaryOffset != nil))) {
							r.Faults = append(r.Faults, Stamp{"clock_check_held", host(), pid})
							stallDone = true
							if s.BoundaryOffset != nil {
								pause(r.Deadline - 5 - s.WallOffset - *s.BoundaryOffset - host())
							} else {
								pause(6)
							}
						}
					}
				}
				if c.Number == uint64(syscall.SYS_KILL) && int64(c.A0) < 0 {
					signal := syscall.Signal(c.A1)
					if signal == syscall.SIGTERM || signal == syscall.SIGKILL {
						r.Signals = append(r.Signals, Stamp{signal.String(), host(), pid})
					}
					c.Denied = (signal == syscall.SIGTERM && s.DenyTerm) || (signal == syscall.SIGKILL && s.DenyKill)
					if c.Denied {
						setreg(&regs, 0, 2147483647)
						if e = syscall.PtraceSetRegs(pid, &regs); e != nil && !vanished(e) {
							r.Error = e.Error()
							return
						}
						r.Faults = append(r.Faults, Stamp{"signal_denied", host(), pid})
					}
				}
				calls[pid] = c
			} else {
				entry[pid] = false
				c := calls[pid]
				if c.Clock && retval(&regs) == 0 {
					offset := 0.0
					if c.ClockID == 0 {
						offset = s.WallOffset
					}
					if c.ClockID == 1 && s.WideBrackets && tgid(pid) == root && monos[root]%2 == 0 {
						offset = 0.1
					}
					if offset != 0 {
						if e = patchClock(pid, c.Pointer, offset); e != nil && !vanished(e) {
							r.Error = e.Error()
							return
						}
					}
				}
				if c.Denied {
					setreturn(&regs, -int64(syscall.EPERM))
					if e = syscall.PtraceSetRegs(pid, &regs); e != nil && !vanished(e) {
						r.Error = e.Error()
						return
					}
				}
				if c.Number == uint64(syscall.SYS_KILL) && c.A1 == 0 && s.TransientProbe && retval(&regs) == -int64(syscall.ESRCH) && probeCount < 3 {
					probeCount++
					setreturn(&regs, -int64(syscall.EPERM))
					if e = syscall.PtraceSetRegs(pid, &regs); e != nil && !vanished(e) {
						r.Error = e.Error()
						return
					}
				}
			}
			if e = syscall.PtraceSyscall(pid, 0); e != nil && e != syscall.ESRCH {
				r.Error = e.Error()
				return
			}
			continue
		}
		forward := int(sig)
		if sig == syscall.SIGSTOP || sig == syscall.SIGTRAP {
			forward = 0
		}
		if e = syscall.PtraceSyscall(pid, forward); e != nil && e != syscall.ESRCH {
			r.Error = e.Error()
			return
		}
	}
	r.CleanupDone = true
	r.RequirementMet = (r.Exec == 0 || r.Exec <= r.Cutoff) && (!s.Stubborn || r.LastGroupExit > 0 && r.LastGroupExit <= r.Deadline+60 && !r.HarnessCleanup)
	r.Pass = r.Error == "" && r.Exit == s.WantExit && (r.Exec > 0) == s.WantExec
	if s.ExpectedRequirementFailure {
		r.Pass = r.Pass && !r.RequirementMet
	} else if !s.DenyKill {
		r.Pass = r.Pass && r.RequirementMet
	}
	if s.DenyKill {
		r.Pass = r.Pass && r.HarnessCleanup
	}
	if s.Stall != "" || s.BoundaryOffset != nil {
		r.Pass = r.Pass && stallDone
	}
	return
}

func main() {
	runtime.LockOSThread()
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: deadline_trace installed_binary scenarios.json results.json")
		os.Exit(2)
	}
	raw, err := os.ReadFile(os.Args[2])
	if err != nil {
		panic(err)
	}
	var cases []Scenario
	if err = json.Unmarshal(raw, &cases); err != nil {
		panic(err)
	}
	output := []Result{}
	all := true
	for _, s := range cases {
		r := verify(os.Args[1], s)
		output = append(output, r)
		all = all && r.Pass && r.CleanupDone
		fmt.Fprintf(os.Stderr, "%s pass=%v exit=%d error=%s\n", r.Name, r.Pass, r.Exit, r.Error)
		b, _ := json.MarshalIndent(output, "", "  ")
		if err = os.WriteFile(os.Args[3], b, 0644); err != nil {
			panic(err)
		}
	}
	if !all {
		os.Exit(1)
	}
}
