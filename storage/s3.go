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
	"strings"
	"sync"
	"time"
)

const s3ContentType = "application/binary"
var s3Options = s3.Options{}

type S3 struct {
	auth      aws.Auth
	authLock  sync.RWMutex
	region    aws.Region
	s3        *s3.S3
	bucket    *s3.Bucket
	bufferDir *BufferDir
	rootPath  string // sanitized root path (no leading slash)

	Region    string `json:"region"`
	Bucket    string `json:"bucket"`
	RootPath  string `json:"root_path"`
	BufferDir string `json:"buffer_dir"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
}

func (s *S3) GetRootPath() string {
	return s.RootPath
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
	if s.BufferDir == "" {
		return errors.New("Please Specify an S3 Buffer Directory")
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
	if err := os.Mkdir(s.BufferDir, 0755); err != nil && !os.IsExist(err) {
		// there was an error and it wasn't that the directory already exists
		return err
	}
	s.bufferDir = &BufferDir{Mutex: sync.Mutex{}, root: s.BufferDir}
	s.rootPath = strings.TrimPrefix(s.RootPath, "/")
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
	return s.bucket.Get(s.key(path))
}

func (s *S3) PutContent(path string, content []byte) error {
	s.authLock.RLock()
	defer s.authLock.RUnlock()
	return s.bucket.Put(s.key(path), content, s3ContentType, s3.Private, s3Options)
}

func (s *S3) StreamRead(path string) (io.ReadCloser, error) {
	s.authLock.RLock()
	defer s.authLock.RUnlock()
	return s.bucket.GetReader(s.key(path))
}

func (s *S3) StreamWrite(path string, reader io.Reader) error {
	key := s.key(path)
	buffer, err := s.bufferDir.reserve(key)
	if err != nil {
		return err
	}
	defer buffer.release()
	// don't know the length, buffer to file first
	length, err := io.Copy(buffer, reader)
	if err != nil {
		return err
	}
	buffer.Seek(0, 0) // seek to the beginning of the file
	// we know the length, write to s3 from file now
	return s.bucket.PutReader(s.key(path), buffer, length, s3ContentType, s3.Private, s3Options)
}

func (s *S3) ListDirectory(path string) ([]string, error) {
	s.authLock.RLock()
	defer s.authLock.RUnlock()
	result, err := s.bucket.List(s.key(path) + "/", "/", "", 0)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(result.Contents)+len(result.CommonPrefixes))
	for i, key := range result.Contents {
		// return path RootPath instead of sanitized version to be consistent
		names[i] = s.RootPath + strings.TrimPrefix(key.Key, s.rootPath)
	}
	for i, prefix := range result.CommonPrefixes {
		// return path RootPath instead of sanitized version to be consistent, also trim trailing "/"
		names[i+len(result.Contents)] = s.RootPath + strings.TrimPrefix(strings.TrimSuffix(prefix, "/"), s.rootPath)
	}
	return names, nil
}

func (s *S3) Exists(path string) (bool, error) {
	s.authLock.RLock()
	defer s.authLock.RUnlock()
	return s.bucket.Exists(s.key(path))
}

func (s *S3) Remove(path string) error {
	s.authLock.RLock()
	defer s.authLock.RUnlock()
	return s.bucket.Del(s.key(path))
}

func (s *S3) RemoveAll(path string) error {
	// find and remove everything "under" it
	s.authLock.RLock()
	defer s.authLock.RUnlock()
	result, err := s.bucket.List(s.key(path) + "/", "", "", 0)
	if err != nil {
		return err
	}
	for _, key := range result.Contents {
		s.bucket.Del(key.Key)
	}
	// finally, remove it
	return s.Remove(path)
}

func (s *S3) key(path string) string {
	return p.Join(s.rootPath, path) // s3 expects no leading slash in some operations
}

// This will ensure that we don't try to upload the same thing from two different requests at the same time
type BufferDir struct {
	sync.Mutex
	root string
}

func (b *BufferDir) reserve(key string) (*Buffer, error) {
	b.Lock()
	defer b.Unlock()
	// sha key path and create temporary file
	filePath := p.Join(b.root, fmt.Sprintf("%x", sha256.Sum256([]byte(key))))
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		// buffer file already exists
		return nil, errors.New("Upload already in progress for key "+key)
	}
	// if not exist, create buffer file
	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	return &Buffer{File: *file, dir: b}, nil
}

type Buffer struct {
	os.File
	dir *BufferDir
}
func (b* Buffer) release() error {
	b.dir.Lock()
	defer b.dir.Unlock()
	b.Close()
	return os.Remove(b.Name())
}
