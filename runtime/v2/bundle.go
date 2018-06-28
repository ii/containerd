package v2

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/containerd/containerd/namespaces"
)

const configFilename = "config.json"

// LoadBundle loads an existing bundle from disk
func LoadBundle(ctx context.Context, root, id string) (*Bundle, error) {
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return nil, err
	}
	return &Bundle{
		ID:   id,
		Path: filepath.Join(root, ns, id),
	}, nil
}

// NewBundle returns a new bundle on disk
func NewBundle(ctx context.Context, root, state, id string, spec []byte) (b *Bundle, err error) {
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return nil, err
	}
	work := filepath.Join(state, ns, id)
	b = &Bundle{
		ID:   id,
		Path: filepath.Join(root, ns, id),
	}
	paths := []string{b.Path, work}
	defer func() {
		if err != nil {
			for _, d := range paths {
				os.RemoveAll(d)
			}
		}
	}()
	// create base directories
	for _, d := range paths {
		if err := os.MkdirAll(filepath.Dir(d), 0711); err != nil {
			return nil, err
		}
	}
	for _, d := range paths {
		if err := os.Mkdir(d, 0711); err != nil {
			return nil, err
		}
	}
	// create rootfs dir
	if err := os.Mkdir(filepath.Join(b.Path, "rootfs"), 0711); err != nil {
		return nil, err
	}
	// symlink workdir
	if err := os.Symlink(b.WorkDir, filepath.Join(b.Path, "work")); err != nil {
		return nil, err
	}
	// write the spec to the bundle
	err = ioutil.WriteFile(filepath.Join(b.Path, configFilename), spec, 0666)
	return b, err
}

type Bundle struct {
	// ID of the bundle
	ID string
	// Path to the bundle
	Path string
}

// Delete a bundle atomically
func (b *Bundle) Delete() error {
	work, err := os.Readlink(filepath.Join(b.Path, "work"))
	if err != nil {
		return err
	}
	err := os.RemoveAll(b.Path)
	if err == nil {
		return os.RemoveAll(work)
	}
	// error removing the bundle path; still attempt removing work dir
	err2 := os.RemoveAll(work)
	if err2 == nil {
		return err
	}
	return errors.Wrapf(err, "failed to remove both bundle and workdir locations: %v", err2)
}
