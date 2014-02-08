package storage

import (
	"io"
	"io/ioutil"
	p "path"
	"path/filepath"
	"os"
)

type Local struct {
	RootPath string `json:"root_path"`
}

func (s *Local) GetContent(path string) ([]byte, error) {
	return ioutil.ReadFile(p.Join(s.RootPath, path))
}

func (s *Local) PutContent(path string, content []byte) error {
	absPath := p.Join(s.RootPath, path)
	os.MkdirAll(filepath.Dir(absPath), 0770)
	return ioutil.WriteFile(absPath, content, 0660)
}

func (s *Local) StreamRead(path string) (io.ReadCloser, error) {
	return os.Open(p.Join(s.RootPath, path))
}

func (s *Local) StreamWrite(path string, reader io.Reader, length int64) error {
	file, err := os.Create(p.Join(s.RootPath, path))
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(file, reader)
	return err
}

func (s *Local) ListDirectory(path string) ([]string, error) {
	files, err := ioutil.ReadDir(p.Join(s.RootPath, path))
	if err != nil {
		return nil, err
	}

	names := make([]string, len(files))
	for i, f := range files {
		names[i] = p.Join(s.RootPath, path, f.Name())
	}
	return names, nil
}

func (s *Local) Exists(path string) (bool, error) {
	_, err := os.Stat(p.Join(s.RootPath, path))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (s *Local) Remove(path string) error {
	return os.RemoveAll(p.Join(s.RootPath, path))
}
