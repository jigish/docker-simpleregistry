package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

const REPOSITORIES = "repositories"
const IMAGES = "images"
const BUFFER_SIZE = 4096

func ImagesListPath(namespace string, repository string) string {
	return fmt.Sprintf("%s/%s/%s/_images_list", REPOSITORIES, namespace, repository)
}

func ImageJsonPath(image_id string) string {
	return fmt.Sprintf("%s/%s/json", IMAGES, image_id)
}

func ImageMarkPath(image_id string) string {
	return fmt.Sprintf("%s/%s/_inprogress", IMAGES, image_id)
}

func ImageChecksumPath(image_id string) string {
	return fmt.Sprintf("%s/%s/_checksum", IMAGES, image_id)
}

func ImageLayerPath(image_id string) string {
	return fmt.Sprintf("%s/%s/layer", IMAGES, image_id)
}

func ImageAncestryPath(image_id string) string {
	return fmt.Sprintf("%s/%s/ancestry", IMAGES, image_id)
}

func TagPath(namespace string, repository string) string {
	return fmt.Sprintf("%s/%s/%s", REPOSITORIES, namespace, repository)
}

func TagPathWithName(namespace string, repository string, tagname string) string {
	return fmt.Sprintf("%s/%s/%s/tag_%s", REPOSITORIES, namespace, repository, tagname)
}

func ImageListPath(namespace string, repository string) string {
	return fmt.Sprintf("%s/%s/%s/images", REPOSITORIES, namespace, repository)
}

type Storage interface {
	GetContent(string) ([]byte, error)
	PutContent(string, []byte) error
	StreamRead(string) (io.ReadCloser, error)
	StreamWrite(string, io.Reader) error
	ListDirectory(string) ([]string, error)
	Exists(string) (bool, error)
	Remove(string) error
	RemoveAll(string) error

	// used for testing
	GetRootPath() string
}

type Config struct {
	Type  string `json:"type"`
	Local *Local `json:"local"`
	S3    *S3    `json:"s3"`
}

var Default = &Local{"."}

func New(cfgFile string) (Storage, error) {
	// load config from file
	file, err := os.Open(cfgFile)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(file)
	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		return nil, err
	}

	switch cfg.Type {
	case "local":
		if cfg.Local != nil {
			return cfg.Local, cfg.Local.init()
		}
		return nil, errors.New("No config for storage type 'local' found")
	case "s3":
		if cfg.S3 != nil {
			return cfg.S3, cfg.S3.init()
		}
		return nil, errors.New("No config for storage type 's3' found")
	default:
		return nil, errors.New("Invalid storage type: "+cfg.Type)
	}
}
