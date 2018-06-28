package v2

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/containerd/containerd/api/types"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/containerd/runtime"
	"github.com/containerd/containerd/runtime/v2/task"
	"github.com/containerd/containerd/sys"
	"github.com/containerd/ttrpc"
	"github.com/docker/swarmkit/log"
	"github.com/sirupsen/logrus"
)

const shimBinaryFormat = "containerd-shim-%s"

// NewShim starts and returns a new shim
func NewShim(ctx context.Context, bundle *Bundle, runtime, containerdAddress string) (_ *Shim, err error) {
	address, err := abstractAddress(ctx, bundle.ID)
	if err != nil {
		return nil, err
	}
	socket, err := newSocket(address)
	if err != nil {
		return nil, err
	}
	defer socket.Close()
	f, err := socket.File()
	if err != nil {
		return nil, err
	}
	defer f.Close()

	cmd, err := shimCommand(ctx, runtime, containerdAddress, bundle)
	if err != nil {
		return nil, err
	}
	cmd.ExtraFiles = append(cmd.ExtraFiles, f)

	if err := cmd.Start(); err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			cmd.Process.Kill()
		}
	}()
	// make sure to wait after start
	go cmd.Wait()
	if err := writePidFile(filepath.Join(bundle.Path, "shim.pid"), cmd.Process.Pid); err != nil {
		return nil, err
	}
	log.G(ctx).WithFields(logrus.Fields{
		"pid":     cmd.Process.Pid,
		"address": address,
	}).Infof("shim %s started", cmd.Args[0])
	if err = sys.SetOOMScore(cmd.Process.Pid, sys.OOMScoreMaxKillable); err != nil {
		return nil, errors.Wrap(err, "failed to set OOM Score on shim")
	}
	// write pid file

	conn, err := connect(address, annonDialer)
	if err != nil {
		return nil, err
	}
	client := ttrpc.NewClient(conn)
	client.OnClose(conn.Close)
	return &Shim{
		bundle:  bundle,
		client:  client,
		task:    task.NewTaskClient(client),
		shimPid: cmd.Process.Pid,
	}, nil
}

type Shim struct {
	bundle  *Bundle
	client  *ttrpc.Client
	task    task.TaskClient
	shimPid int
	taskPid int
}

// ID of the shim/task
func (s *Shim) ID() string {
	return s.bundle.ID
}

func (s *Shim) Close() error {
	return s.client.Close()
}

func (s *Shim) Create(ctx context.Context, opts runtime.CreateOpts) (Task, error) {
	request := &task.CreateTaskRequest{
		ID:         s.ID(),
		Bundle:     s.bundle.Path,
		Stdin:      opts.IO.Stdin,
		Stdout:     opts.IO.Stdout,
		Stderr:     opts.IO.Stderr,
		Terminal:   opts.IO.Terminal,
		Checkpoint: opts.Checkpoint,
		Options:    opts.Options,
	}
	for _, m := range opts.Rootfs {
		sopts.Rootfs = append(sopts.Rootfs, &types.Mount{
			Type:    m.Type,
			Source:  m.Source,
			Options: m.Options,
		})
	}
	response, err := s.task.Create(ctx, request)
	if err != nil {
		return nil, err
	}
	s.taskPid = int(response.Pid)
	return s, nil
}

func shimCommand(ctx context.Context, runtime, containerdAddress string, bundle *Bundle) (*exec.Cmd, error) {
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return nil, err
	}
	self, err := os.Executable()
	if err != nil {
		return nil, err
	}
	args := []string{
		"-namespace": ns,
		"-address":   containerdAddress,
		"-containerd-binary", self,
	}
	cmd := exec.Command(getShimBinaryName(runtime), args...)
	cmd.Dir = bundle.Path
	cmd.Env = append(os.Environ(), "GOMAXPROCS=2")
	cmd.SysProcAttr = getSysProcAttr()
	return cmd, nil
}

func getShimBinaryName(runtime string) string {
	parts := strings.Split(runtime, ".")
	return fmt.Sprintf(shimBinaryFormat, parts[len(parts)-1])
}

func abstractAddress(ctx context.Context, id string) (string, error) {
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return "", err
	}
	return filepath.Join(string(filepath.Separator), "containerd-shim", ns, id, "shim.sock"), nil
}

func connect(address string, d func(string, time.Duration) (net.Conn, error)) (net.Conn, error) {
	return d(address, 100*time.Second)
}

func annonDialer(address string, timeout time.Duration) (net.Conn, error) {
	address = strings.TrimPrefix(address, "unix://")
	return net.DialTimeout("unix", "\x00"+address, timeout)
}

func newSocket(address string) (*net.UnixListener, error) {
	if len(address) > 106 {
		return nil, errors.Errorf("%q: unix socket path too long (> 106)", address)
	}
	l, err := net.Listen("unix", "\x00"+address)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to listen to abstract unix socket %q", address)
	}
	return l.(*net.UnixListener), nil
}

func writePidFile(path string, pid int) error {
	path, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	tempPath := filepath.Join(filepath.Dir(path), fmt.Sprintf(".%s", filepath.Base(path)))
	f, err := os.OpenFile(tempPath, os.O_RDWR|os.O_CREATE|os.O_EXCL|os.O_SYNC, 0666)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(f, "%d", pid)
	f.Close()
	if err != nil {
		return err
	}
	return os.Rename(tempPath, path)
}
