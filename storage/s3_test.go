package storage

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

var s3Storage *S3

func setupS3() error {
	if s3Storage != nil {
		return nil
	}
	// read test config
	s3Filename := os.Getenv("S3_CONFIG_FILE")
	s3File, err := os.Open(s3Filename)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(s3File)
	var s3S S3
	if err := dec.Decode(&s3S); err != nil {
		return err
	}
	if err := (&s3S).init(); err != nil {
		return err
	}
	s3Storage = &s3S
	return nil
}

func TestS3(t *testing.T) {
	if err := setupS3(); err != nil {
		t.Fatal(err)
	}

	// remove all and list
	if err := s3Storage.RemoveAll("/"); err != nil {
		t.Fatal(err)
	}
	if keys, err := s3Storage.ListDirectory("/"); err != nil {
		t.Fatal(err)
	} else if len(keys) > 0 {
		t.Log("There shouldn't be any keys yet")
		t.FailNow()
	}

	// put one and list
	if err := s3Storage.PutContent("/1", []byte("lolwtf")); err != nil {
		t.Fatal(err)
	}
	if content, err := s3Storage.GetContent("/1"); err != nil {
		t.Fatal(err)
	} else if string(content) != "lolwtf" {
		t.Log("The content should be 'lolwtf' was '"+string(content)+"'")
		t.FailNow()
	}
	if keys, err := s3Storage.ListDirectory("/"); err != nil {
		t.Fatal(err)
	} else if len(keys) != 1 {
		t.Log("There should be a key")
		t.FailNow()
	} else if keys[0] != path.Join(s3Storage.RootPath, "1") {
		t.Log("The key should be '"+path.Join(s3Storage.RootPath, "1")+"' was '"+keys[0]+"'")
		t.FailNow()
	}

	// put another and list
	if err := s3Storage.PutContent("/2", []byte("lolwtf2")); err != nil {
		t.Fatal(err)
	}
	if content, err := s3Storage.GetContent("/2"); err != nil {
		t.Fatal(err)
	} else if string(content) != "lolwtf2" {
		t.Log("The content should be 'lolwtf2' was '"+string(content)+"'")
		t.FailNow()
	}
	if keys, err := s3Storage.ListDirectory("/"); err != nil {
		t.Fatal(err)
	} else if len(keys) != 2 {
		t.Log("There should be 2 keys")
		t.FailNow()
	} else if keys[0] != path.Join(s3Storage.RootPath, "1") {
		t.Log("The key[0] should be '"+path.Join(s3Storage.RootPath, "1")+"' was '"+keys[0]+"'")
		t.FailNow()
	} else if keys[1] != path.Join(s3Storage.RootPath, "2") {
		t.Log("The key[1] should be '"+path.Join(s3Storage.RootPath, "2")+"' was '"+keys[1]+"'")
		t.FailNow()
	}

	// remove second and list
	if err := s3Storage.Remove("/2"); err != nil {
		t.Fatal(err)
	}
	if keys, err := s3Storage.ListDirectory("/"); err != nil {
		t.Fatal(err)
	} else if len(keys) != 1 {
		t.Log("There should be a key")
		t.FailNow()
	} else if keys[0] != path.Join(s3Storage.RootPath, "1") {
		t.Log("The key should be '"+path.Join(s3Storage.RootPath, "1")+"' was '"+keys[0]+"'")
		t.FailNow()
	}

	// stream write in a dir and list
	if err := s3Storage.StreamWrite("/dir/1", bytes.NewBufferString("lolwtfdir")); err != nil {
		t.Fatal(err)
	}
	if reader, err := s3Storage.StreamRead("/dir/1"); err != nil {
		t.Fatal(err)
	} else {
		content, err := ioutil.ReadAll(reader)
		reader.Close()
		if err != nil {
			t.Fatal(err)
		}
		if string(content) != "lolwtfdir" {
			t.Log("The content should be 'lolwtfdir' was '"+string(content)+"'")
			t.FailNow()
		}
	}
	if keys, err := s3Storage.ListDirectory("/"); err != nil {
		t.Fatal(err)
	} else if len(keys) != 2 {
		t.Log("There should be two keys")
		t.FailNow()
	} else if keys[0] != path.Join(s3Storage.RootPath, "1") {
		t.Log("The key should be '"+path.Join(s3Storage.RootPath, "1")+"' was '"+keys[0]+"'")
		t.FailNow()
	} else if keys[1] != path.Join(s3Storage.RootPath, "dir") {
		t.Log("The key should be '"+path.Join(s3Storage.RootPath, "dir")+"' was '"+keys[1]+"'")
		t.FailNow()
	}

	// remove all and list
	if err := s3Storage.RemoveAll("/"); err != nil {
		t.Fatal(err)
	}
	if keys, err := s3Storage.ListDirectory("/"); err != nil {
		t.Fatal(err)
	} else if len(keys) > 0 {
		t.Log("There shouldn't be any keys yet")
		t.FailNow()
	}
}
