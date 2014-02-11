package storage

import (
	"encoding/json"
	"os"
	"testing"
)

func setupLocal() (Storage, error) {
	// read test config
	localFilename := os.Getenv("LOCAL_CONFIG_FILE")
	localFile, err := os.Open(localFilename)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(localFile)
	var local Local
	if err := dec.Decode(&local); err != nil {
		return nil, err
	}
	if err := (&local).init(); err != nil {
		return nil, err
	}
	return &local, nil
}

func TestLocal(t *testing.T) {
	testStorage(t, setupLocal)
}

