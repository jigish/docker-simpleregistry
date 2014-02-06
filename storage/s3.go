package storage

import (
	"errors"
	"io"
)

type S3 struct {
	// TODO add fields here
}

func (s *S3) GetContent(path string) ([]byte, error) {
	return nil, errors.New("Unimplemented")
}

func (s *S3) PutContent(path string, content []byte) error {
	return errors.New("Unimplemented")
}

func (s *S3) StreamRead(path string) (io.ReadCloser, error) {
	return nil, errors.New("Unimplemented")
}

func (s *S3) StreamWrite(path string) (io.WriteCloser, error) {
	return nil, errors.New("Unimplemented")
}

func (s *S3) ListDirectory(path string) ([]string, error) {
	return nil, errors.New("Unimplemented")
}

func (s *S3) Exists(path string) (bool, error) {
	return false, errors.New("Unimplemented")
}

func (s *S3) Remove(path string) error {
	return errors.New("Unimplemented")
}
