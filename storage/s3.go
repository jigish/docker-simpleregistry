package storage

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"io"
	"os"
	p "path"
	"sync"
	"time"
)

const s3ContentType = "application/binary"
const s3BufferDir = "/tmp/docker-registry"
var s3Options = s3.Options{}

type S3 struct {
	auth      aws.Auth
	authLock  sync.RWMutex
	region    aws.Region
	s3        *s3.S3
	bucket    *s3.Bucket

	Region    string `json:"region"`
	Bucket    string `json:"bucket"`
	RootPath  string `json:"root_path"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
}

func (s *S3) getAuth() (err error) {
	s.auth, err = aws.GetAuth(s.AccessKey, s.SecretKey, "", time.Time{})
	if s.s3 != nil {
		s.s3.Auth = s.auth
	}
	return
}

func (s *S3) init() error {
	if s.Bucket == "" {
		return errors.New("Please Specify an S3 Bucket")
	}
	if s.Region == "" {
		return errors.New("Please Specify an S3 Region")
	}
	if s.RootPath == "" {
		return errors.New("Please Specify an S3 Root Path")
	}

	var ok bool
	if s.region, ok = aws.Regions[s.Region]; !ok {
		return errors.New("Invalid Region: "+s.Region)
	}
	err := s.getAuth()
	if err != nil {
		return err
	}
	s.s3 = s3.New(s.auth, s.region)
	s.bucket = s.s3.Bucket(s.Bucket)
	if err := os.Mkdir(s3BufferDir, 0755); err != nil && !os.IsExist(err) {
		// there was an error and it wasn't that the directory already exists
		return err
	}
	go s.updateAuthLoop()
	return nil
}

func (s *S3) updateAuth() {
	s.authLock.Lock()
	defer s.authLock.Unlock()
	err := s.getAuth()
	for ; err != nil; err = s.getAuth() {
		time.Sleep(5 * time.Second)
	}
}

func (s *S3) updateAuthLoop() {
	// this function just updates the auth. s.auth should be set before this is called
	for {
		if s.auth.Expiration().IsZero() {
			// no reason to update, expiration is zero.
			return
		}
		if diff := s.auth.Expiration().Sub(time.Now()); diff < 0 {
			// if we're past the expiration time, update the auth
			s.updateAuth()
		} else {
			// if we're not past the expiration time, sleep until the expiration time is up
			time.Sleep(diff)
		}
	}
}

func (s *S3) GetContent(path string) ([]byte, error) {
	s.authLock.RLock()
	defer s.authLock.RUnlock()
	return s.bucket.Get(p.Join(s.RootPath, path))
}

func (s *S3) PutContent(path string, content []byte) error {
	s.authLock.RLock()
	defer s.authLock.RUnlock()
	return s.bucket.Put(p.Join(s.RootPath, path), content, s3ContentType, s3.Private, s3Options)
}

func (s *S3) StreamRead(path string) (io.ReadCloser, error) {
	s.authLock.RLock()
	defer s.authLock.RUnlock()
	return s.bucket.GetReader(p.Join(s.RootPath, path))
}

func (s *S3) StreamWrite(path string, reader io.Reader, length int64) error {
	key := p.Join(s.RootPath, path)
	buffer, bufferPath, err := reserveTmpFile(key)
	if err != nil {
		return err
	}
	readFrom := reader
	if length < 0 {
		// don't know the length, buffer to file first
		length, err = io.Copy(buffer, reader)
		buffer.Close() // close buffer file to flush
		if err != nil {
			releaseTmpFile(bufferPath)
			return err
		}
		buffer, err = os.Open(bufferPath) // re-open buffer file for reading
		if err != nil {
			releaseTmpFile(bufferPath)
			return err
		}
		defer buffer.Close() // make sure to close the buffer file
		readFrom = buffer // instead of reading from the reader we got, now read from the buffer file
	}
	defer releaseTmpFile(bufferPath) // this defer needs to happen *after* the buffer.Close() above it
	// we know the length, write now
	return s.bucket.PutReader(p.Join(s.RootPath, path), readFrom, length, s3ContentType, s3.Private, s3Options)
}

func (s *S3) ListDirectory(path string) ([]string, error) {
	s.authLock.RLock()
	defer s.authLock.RUnlock()
	result, err := s.bucket.List(p.Join(s.RootPath, path) + "/", "/", "", 0)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(result.Contents))
	for i, key := range result.Contents {
		names[i] = key.Key // TODO verify this comes back properly
	}
	return names, nil
}

func (s *S3) Exists(path string) (bool, error) {
	s.authLock.RLock()
	defer s.authLock.RUnlock()
	return s.bucket.Exists(p.Join(s.RootPath, path))
}

func (s *S3) Remove(path string) error {
	s.authLock.RLock()
	defer s.authLock.RUnlock()
	// TODO do i have to remove all with this prefix here?
	return s.bucket.Del(p.Join(s.RootPath, path))
}

// This will ensure that we don't try to upload the same thing from two different requests at the same time
var tmpDirLock = sync.Mutex{}
func reserveTmpFile(key string) (*os.File, string, error) {
	tmpDirLock.Lock()
	defer tmpDirLock.Unlock()
	// sha key path and create temporary file
	bufferPath := p.Join(s3BufferDir, fmt.Sprintf("%x", sha256.Sum256([]byte(key))))
	if _, err := os.Stat(bufferPath); !os.IsNotExist(err) {
		// buffer file already exists
		return nil, "", errors.New("Upload already in progress for key "+key)
	}
	// if not exist, create buffer file
	buffer, err := os.Create(bufferPath)
	if err != nil {
		return nil, "", err
	}
	return buffer, bufferPath, nil
}

func releaseTmpFile(fileName string) error {
	tmpDirLock.Lock()
	defer tmpDirLock.Unlock()
	return os.Remove(fileName)
}
