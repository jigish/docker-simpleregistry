package storage

import (
	"bytes"
	"io/ioutil"
	"path"
	"testing"
)

func testStorage(t *testing.T, setup func() (Storage, error)) {
	storage, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	// remove all and list
	if err := storage.RemoveAll("/"); err != nil {
		t.Fatal(err)
	}
	if keys, err := storage.ListDirectory("/"); err != nil {
		t.Fatal(err)
	} else if len(keys) > 0 {
		t.Log("There shouldn't be any keys yet")
		t.FailNow()
	}

	// put one and list
	if err := storage.PutContent("/1", []byte("lolwtf")); err != nil {
		t.Fatal(err)
	}
	if content, err := storage.GetContent("/1"); err != nil {
		t.Fatal(err)
	} else if string(content) != "lolwtf" {
		t.Log("The content should be 'lolwtf' was '"+string(content)+"'")
		t.FailNow()
	}
	if keys, err := storage.ListDirectory("/"); err != nil {
		t.Fatal(err)
	} else if len(keys) != 1 {
		t.Log("There should be a key")
		t.FailNow()
	} else if keys[0] != path.Join(storage.GetRootPath(), "1") {
		t.Log("The key should be '"+path.Join(storage.GetRootPath(), "1")+"' was '"+keys[0]+"'")
		t.FailNow()
	}

	// put another and list
	if err := storage.PutContent("/2", []byte("lolwtf2")); err != nil {
		t.Fatal(err)
	}
	if content, err := storage.GetContent("/2"); err != nil {
		t.Fatal(err)
	} else if string(content) != "lolwtf2" {
		t.Log("The content should be 'lolwtf2' was '"+string(content)+"'")
		t.FailNow()
	}
	if keys, err := storage.ListDirectory("/"); err != nil {
		t.Fatal(err)
	} else if len(keys) != 2 {
		t.Log("There should be 2 keys")
		t.FailNow()
	} else if keys[0] != path.Join(storage.GetRootPath(), "1") {
		t.Log("The key[0] should be '"+path.Join(storage.GetRootPath(), "1")+"' was '"+keys[0]+"'")
		t.FailNow()
	} else if keys[1] != path.Join(storage.GetRootPath(), "2") {
		t.Log("The key[1] should be '"+path.Join(storage.GetRootPath(), "2")+"' was '"+keys[1]+"'")
		t.FailNow()
	}

	// remove second and list
	if err := storage.Remove("/2"); err != nil {
		t.Fatal(err)
	}
	if keys, err := storage.ListDirectory("/"); err != nil {
		t.Fatal(err)
	} else if len(keys) != 1 {
		t.Log("There should be a key")
		t.FailNow()
	} else if keys[0] != path.Join(storage.GetRootPath(), "1") {
		t.Log("The key should be '"+path.Join(storage.GetRootPath(), "1")+"' was '"+keys[0]+"'")
		t.FailNow()
	}

	// stream write in a dir and list
	if err := storage.StreamWrite("/dir/1", bytes.NewBufferString("lolwtfdir")); err != nil {
		t.Fatal(err)
	}
	if reader, err := storage.StreamRead("/dir/1"); err != nil {
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
	if keys, err := storage.ListDirectory("/"); err != nil {
		t.Fatal(err)
	} else if len(keys) != 2 {
		t.Log("There should be two keys")
		t.FailNow()
	} else if keys[0] != path.Join(storage.GetRootPath(), "1") {
		t.Log("The key should be '"+path.Join(storage.GetRootPath(), "1")+"' was '"+keys[0]+"'")
		t.FailNow()
	} else if keys[1] != path.Join(storage.GetRootPath(), "dir") {
		t.Log("The key should be '"+path.Join(storage.GetRootPath(), "dir")+"' was '"+keys[1]+"'")
		t.FailNow()
	}

	// remove all and list
	if err := storage.RemoveAll("/"); err != nil {
		t.Fatal(err)
	}
	if keys, err := storage.ListDirectory("/"); err != nil {
		t.Fatal(err)
	} else if len(keys) > 0 {
		t.Log("There shouldn't be any keys yet")
		t.FailNow()
	}
}


