package v2

import (
	"context"
	"os"

	"github.com/containerd/containerd/events/exchange"
	"github.com/containerd/containerd/runtime"
	ptypes "github.com/gogo/protobuf/types"
)

var empty = &ptypes.Empty{}

func New(root, state, containerdAddress string, monitor runtime.TaskMonitor, events *exchange.Exchange) (*TaskManager, error) {
	for _, d := range []string{root, state} {
		if err := os.MkdirAll(d, 0711); err != nil {
			return nil, err
		}
	}
	return &TaskManager{
		root:              root,
		state:             state,
		containerdAddress: containerdAddress,
		monitor:           monitor,
		tasks:             runtime.NewTaskList(),
		events:            events,
	}, nil
}

type TaskManager struct {
	root              string
	state             string
	containerdAddress string

	monitor runtime.TaskMonitor
	tasks   *runtime.TaskList
	events  *exchange.Exchange
}

func (m *TaskManager) ID() string {
	return "io.containerd.task.v2"
}

func (m *TaskManager) Create(ctx context.Context, id string, opts runtime.CreateOpts) (_ runtime.Task, err error) {
	bundle, err := NewBundle(ctx, m.root, m.state, id, opts.Spec.Value)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			bundle.Delete()
		}
	}()
	shim, err := NewShim(ctx, bundle, opts.Runtime, m.events)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			shim.Close()
		}
	}()
	task, err := shim.Create(ctx, opts)
	if err != nil {
		return nil, err
	}
	if err := m.tasks.Add(ctx, task); err != nil {
		return nil, err
	}
	// TODO: maybe not fail here?
	if err := m.monitor.Monitor(task); err != nil {
		return nil, err
	}
	return task, nil
}

func (m *TaskManager) Get(ctx context.Context, id string) (runtime.Task, error) {
	return m.tasks.Get(ctx, id)
}

func (m *TaskManager) Tasks(ctx context.Context) ([]runtime.Task, error) {
	return m.tasks.GetAll(ctx)
}

func (m *TaskManager) Delete(ctx context.Context, task runtime.Task) (*runtime.Exit, error) {
	if err := m.monitor.Stop(task); err != nil {
		return nil, err
	}
	shim := task.(*Shim)
	exit, err := shim.Delete(ctx)
	if err != nil {
		return nil, err
	}
	m.tasks.Delete(ctx, task.ID())
	return exit, nil
}
