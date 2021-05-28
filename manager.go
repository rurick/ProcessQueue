package processqueue

import (
	"context"
	"runtime"
	"sync"
	"time"
)

type (
	// ProcKey - unique key of process.
	// you must to provide a unique key value for each process
	ProcKey string

	// map of processes
	procMap map[ProcKey]Process

	// Instance of runing Process
	Process interface {
		// New - Create new instance of process
		New(ctx context.Context) error
		// Run - run process
		Run(ctx context.Context) error
		// Terminate - terminate process. It is abnormal termination of process
		Terminate()
		// OnComplete - this function called when process normally completed and Run function return nil
		OnComplete()
		// OnError - this function called when Run function return error
		OnError(error)
		// OnTimeout - this function called when process terminate by timeout
		OnTimeout()
	}

	Manager struct {
		sync.RWMutex
		capacity              int
		procMap               procMap
		ctx                   context.Context
		procTimeoutCtx        map[ProcKey]context.Context
		procTimeoutCancelFunc map[ProcKey]context.CancelFunc
	}
)

// New - create a new instance of process queue manager
// c - is maximum capacity of queue. 1 and more
func New(ctx context.Context, c int) *Manager {
	m := new(Manager)
	if c < 1 {
		c = 1
	}
	m.capacity = c
	m.ctx = ctx
	m.procMap = make(procMap)
	return m
}

// Push - add new Process to queue and run one in goroutine
// the function is blocked until there is free space in the queue
// if Manager context is Done, then function return
// timeout - maximum duration for process running
func (m *Manager) Push(key ProcKey, p Process, timeout time.Duration) {
	m.RWMutex.Lock()
	defer m.RWMutex.Unlock()
	for {
		select {
		case <-m.ctx.Done():
			return
		default:
			if len(m.procMap) < m.capacity {
				goto pushEndFor
			}
			time.Sleep(100 * time.Millisecond) // cpu offloading
			runtime.Gosched()
		}
	}
pushEndFor:
	ctx, c := context.WithTimeout(m.ctx, timeout)
	m.procTimeoutCtx[key] = ctx
	m.procTimeoutCancelFunc[key] = c
	m.procMap[key] = p
	go func() {
		if err := p.Run(ctx); err != nil {
			p.OnError(err)
		} else {
			p.OnComplete()
		}
	}()
}

// TerminateAll - terminate all Running Processes
func (m *Manager) TerminateAll() {
	for key, c := range m.procTimeoutCancelFunc {
		c()
		m.procMap[key].Terminate()
	}
}
