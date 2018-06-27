package v2

import (
	"context"

	"github.com/containerd/containerd/runtime"
)

type TaskManager struct {
	root  string
	state string

	monitor runtime.TaskMonitor
	tasks   *runtime.TaskList
}

func (m *TaskManager) ID() string {
	return "io.containerd.task.v2"
}

func (m *TaskManager) Create(ctx context.Context, id string, opts runtime.CreateOpts) (_ runtime.Task, err error) {
	bundle, err := m.newBundle(ctx, id, opts.Spec.Value)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			bundle.Delete()
		}
	}()
	shim, err := m.newShim(bundle, opts.Runtime)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			shim.Close()
		}
	}()
	task, err := shim.Create(opts)
	if err != nil {
		return nil, err
	}
	if err := m.tasks.Add(ctx, task); err != nil {
		return nil, err
	}
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
	exit, err := task.Delete(ctx)
	if err != nil {
		return nil, err
	}
	m.tasks.Delete(ctx, task.ID())
	return exit, nil
}

const shimBinaryFormat = "containerd-shim-%s"

type Bundle struct {
	Path string
}

// finding and exec'ing a new shim
func (m *TaskManager) newShim(ctx context.Context, bundle *Bundle, runtime string) (*Shim, error) {

}
