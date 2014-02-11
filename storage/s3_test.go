package storage

import (
	"encoding/json"
	"os"
	"testing"
)

func setupS3() (Storage, error) {
	// read test config
	s3Filename := os.Getenv("S3_CONFIG_FILE")
	s3File, err := os.Open(s3Filename)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(s3File)
	var sThree S3
	if err := dec.Decode(&sThree); err != nil {
		return nil, err
	}
	if err := (&sThree).init(); err != nil {
		return nil, err
	}
	return &sThree, nil
}

func TestS3(t *testing.T) {
	testStorage(t, setupS3)
}
