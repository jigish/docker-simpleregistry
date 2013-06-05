package main

import (
	"github.com/gorilla/mux"
	"github.com/georgebashi/docker_simpleregistry/storage"
	"net/http"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"encoding/json"
	"crypto/sha256"
	"path/filepath"
)

func PingHandler (w http.ResponseWriter, r *http.Request) {
	sendResponse(w, nil, 200, nil, false)
}

func HomeHandler (w http.ResponseWriter, r *http.Request) {
	sendResponse(w, "docker-simpleregistry server", 200, nil, false)
}

type Context struct {
	storage *storage.Storage
}

func (ctx *Context) GetImageLayerHandler (w http.ResponseWriter, r *http.Request) {
	imageId := mux.Vars(r)["imageId"]
	imageReader, err := ctx.storage.StreamRead(storage.ImageLayerPath(imageId))
	if err != nil {
		sendError(404, "image not found", w)
		return
	}
	defer imageReader.Close()

	io.Copy(w, imageReader)
}

func (ctx *Context) PutImageLayerHandler (w http.ResponseWriter, r *http.Request) {
	imageId := mux.Vars(r)["imageId"]
	jsonData, err := ctx.storage.GetContent(storage.ImageJsonPath(imageId))
	if err != nil {
		sendError(404, "Image's JSON not found", w)
		return
	}

	checksum, err := ctx.storage.GetContent(storage.ImageChecksumPath(imageId))
	if err != nil {
		sendError(404, "Image's checksum not found", w)
		return
	}

	layerPath := storage.ImageLayerPath(imageId)
	markPath := storage.ImageMarkPath(imageId)

	if layerExists, err := ctx.storage.Exists(layerPath); layerExists == true && err == nil {
		if markExists, err := ctx.storage.Exists(markPath); markExists == false || err != nil {
			sendError(409, "Image already exists", w)
			return
		}
	}

	writer, err := ctx.storage.StreamWrite(layerPath)
	if err != nil {
		sendError(500, "Couldn't write to layer file", w)
		return
	}

	io.Copy(writer, r.Body)

	checksumParts := strings.Split(string(checksum), ":")
	computedChecksum, err := ctx.computeImageChecksum(checksumParts[0], imageId, jsonData)
	if err != nil || computedChecksum != strings.ToLower(checksumParts[1]) {
		sendError(400, "Checksum mismatch, ignoring the layer", w)
		return
	}

	ctx.storage.Remove(markPath)

	sendResponse(w, nil, http.StatusOK, nil, false)
}

func (ctx *Context) GetImageJsonHandler (w http.ResponseWriter, r *http.Request) {
	imageId := mux.Vars(r)["imageId"]
	data, err := ctx.storage.GetContent(storage.ImageJsonPath(imageId))
	if err != nil {
		sendError(404, "Image not found", w)
		return
	}

	headers := make(map[string]string)
	if checksum, err := ctx.storage.GetContent(storage.ImageChecksumPath(imageId)); err != nil {
		headers["X-Docker-Checksum"] = string(checksum)
	}

	sendResponse(w, data, 200, headers, true)
}

func (ctx *Context) GetImageAncestryHandler(w http.ResponseWriter, r *http.Request) {
	imageId := mux.Vars(r)["imageId"]

	data, err := ctx.storage.GetContent(storage.ImageAncestryPath(imageId))
	if err != nil {
		sendError(404, "Image not found", w)
	}

	sendResponse(w, data, http.StatusOK, nil, true)
}

func (ctx *Context) computeImageChecksum(algo string, imageId string, jsonData []byte) (string, error) {
	if algo != "sha256" {
		return "", fmt.Errorf("bad algorithm %s, only sha256 supported right now", algo)
	}

	hash := sha256.New()
	fmt.Fprintf(hash, "%s\n", jsonData)
	reader, err := ctx.storage.StreamRead(storage.ImageLayerPath(imageId))
	if err != nil {
		return "", fmt.Errorf("couldn't read image for checksumming", algo)
	}
	io.Copy(hash, reader)
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

func (ctx *Context) PutImageJsonHandler(w http.ResponseWriter, r *http.Request) {
	imageId := mux.Vars(r)["imageId"]
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		sendError(500, "Couldn't read request body", w)
		return
	}

	var data map[string]string
	if err := json.Unmarshal(body, &data); err != nil {
		sendError(400, "Invalid JSON", w)
		return
	}

	if _, ok := data["id"]; !ok {
		sendError(400, "Missing Key `id' in JSON", w)
		return
	}

	checksum := r.Header.Get("X-Docker-Checksum")
	if checksum == "" {
		sendError(400, "Missing Image's checksum", w)
		return
	}

	checksumParts := strings.Split(string(checksum), ":")
	if len(checksumParts) != 2 {
		sendError(400, "Invalid checksum format", w)
		return
	}

	if checksumParts[0] != "sha256" {
		sendError(400, "Checksum algorithm not supported", w)
		return
	}

	checksumPath := storage.ImageChecksumPath(imageId)
	ctx.storage.PutContent(checksumPath, []byte(checksum))

	if imageId != data["id"] {
		sendError(400, "JSON data contains invalid id", w)
		return
	}

	parentId, ok := data["parent"]
	exists, err := ctx.storage.Exists(storage.ImageJsonPath(parentId))
	if ok && !exists && err == nil {
		sendError(400, "Image depends on a non existing parent", w)
		return
	}

	jsonPath := storage.ImageJsonPath(imageId)
	markPath := storage.ImageMarkPath(imageId)

	jsonExists, err := ctx.storage.Exists(jsonPath)
	if err != nil {
		sendError(500, "Couldn't check if JSON exists", w)
		return
	}

	markExists, err := ctx.storage.Exists(markPath)
	if err != nil {
		sendError(500, "Couldn't check if mark exists", w)
		return
	}

	if jsonExists && !markExists {
		sendError(409, "Image already exists", w)
	}

	ctx.storage.PutContent(markPath, []byte("true"))
	ctx.storage.PutContent(jsonPath, body)

	ctx.generateAncestry(imageId, parentId)

	sendResponse(w, nil, 200, nil, false)
}

func (ctx *Context) generateAncestry(imageId string, parentId string) error {
	data, err := ctx.storage.GetContent(storage.ImageAncestryPath(parentId))
	if err != nil {
		return err
	}

	var ancestry []string
	if err := json.Unmarshal(data, &ancestry); err != nil {
		return err
	}

	newAncestry := []string{imageId}
	newAncestry = append(newAncestry, ancestry...)

	data, err = json.Marshal(newAncestry)
	if err != nil {
		return err
	}

	ctx.storage.PutContent(storage.ImageAncestryPath(imageId), data)

	return nil
}

func (ctx *Context) GetTagsHandler(w http.ResponseWriter, r *http.Request) {
	namespace := mux.Vars(r)["namespace"]
	repository := mux.Vars(r)["repository"]

	data := make(map[string]string)

	dir, err := ctx.storage.ListDirectory(storage.TagPath(namespace, repository))
	if err != nil {
		sendError(404, "Repository not found", w)
		return
	}

	for _, fname := range dir {
		tagName := filepath.Base(fname)
		if !strings.HasPrefix(tagName, "tag_") {
			continue
		}

		content, err := ctx.storage.GetContent(fname)
		if err != nil {
			continue
		}
		data[tagName[4:]] = string(content)
	}

	sendResponse(w, data, 200, nil, false)
}

func (ctx *Context) GetTagHandler(w http.ResponseWriter, r *http.Request) {
	namespace := mux.Vars(r)["namespace"]
	repository := mux.Vars(r)["repository"]
	tag := mux.Vars(r)["tag"]

	data, err := ctx.storage.GetContent(storage.TagPathWithName(namespace, repository, tag))
	if err != nil {
		sendError(404, "Tag not found", w)
	}

	sendResponse(w, data, 200, nil, false)
}

func (ctx *Context) PutTagHandler(w http.ResponseWriter, r *http.Request) {
	namespace := mux.Vars(r)["namespace"]
	repository := mux.Vars(r)["repository"]
	tag := mux.Vars(r)["tag"]

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		sendError(500, "Couldn't read request body", w)
		return
	}

	var data string
	if err := json.Unmarshal(body, &data); err != nil {
		sendError(400, "Invalid data", w)
		return
	}

	exists, err := ctx.storage.Exists(storage.ImageJsonPath(data))
	if !exists || err != nil {
		sendError(404, "Image not found", w)
		return
	}

	ctx.storage.PutContent(storage.TagPathWithName(namespace, repository, tag), []byte(data))

	sendResponse(w, data, 200, nil, false)
}

func (ctx *Context) DeleteTagHandler(w http.ResponseWriter, r *http.Request) {
	namespace := mux.Vars(r)["namespace"]
	repository := mux.Vars(r)["repository"]
	tag := mux.Vars(r)["tag"]

	err := ctx.storage.Remove(storage.TagPathWithName(namespace, repository, tag))
	if err != nil {
		sendError(404, "Tag not found", w)
		return
	}

	sendResponse(w, true, 200, nil, false)
}

func (ctx *Context) DeleteRepoHandler(w http.ResponseWriter, r *http.Request) {
	namespace := mux.Vars(r)["namespace"]
	repository := mux.Vars(r)["repository"]

	err := ctx.storage.Remove(storage.TagPath(namespace, repository))
	if err != nil {
		sendError(404, "Repository not found", w)
		return
	}

	sendResponse(w, true, 200, nil, false)
}

func LoginHandler(w http.ResponseWriter, r *http.Request) {
	sendResponse(w, true, 200, nil, false)
}

func (ctx *Context) ListImagesHandler(w http.ResponseWriter, r *http.Request) {
	namespace := mux.Vars(r)["namespace"]
	repository := mux.Vars(r)["repository"]

	data, err := ctx.storage.GetContent(storage.ImageListPath(namespace, repository))
	if err != nil {
		sendError(404, "Repository not found", w)
	}

	sendResponse(w, data, 200, nil, false)
}

func (ctx *Context) PutImageHandler(w http.ResponseWriter, r *http.Request) {
	namespace := mux.Vars(r)["namespace"]
	repository := mux.Vars(r)["repository"]

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		sendError(500, "Couldn't read request body", w)
		return
	}

	var data []map[string]string
	if err := json.Unmarshal(body, &data); err != nil {
		sendError(400, "Invalid data", w)
		return
	}

	imageListPath := storage.ImageListPath(namespace, repository)
	imageList, err := ctx.storage.GetContent(imageListPath)
	if err != nil {
		imageList = []byte("[]")
	}
	var images []map[string]string
	err = json.Unmarshal(imageList, &images)
	if err != nil {
		images = []map[string]string{}
	}
	updated := append(images, data...)

	json, err := json.Marshal(updated)
	ctx.storage.PutContent(imageListPath, []byte(json))

	sendResponse(w, data, 200, nil, false)
}


func sendError(status int, msg string, w http.ResponseWriter) {
	w.WriteHeader(status)
	fmt.Fprintf(w, msg)
}

func sendResponse(w http.ResponseWriter, data interface{}, status int, headers map[string]string, raw bool) {
	if data == nil {
		data = true
	}
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "-1")
	w.Header().Set("Content-Type", "application/json")
	
	for k, v := range headers {
		w.Header().Set(k, v)
	}
	
	if raw == false {
		json, err := json.Marshal(data)
		if err != nil {
			fmt.Fprint(w, data)
		} else {
			w.Write(json)
		}
	}
}

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/_ping", PingHandler)
	r.HandleFunc("/", HomeHandler)

	ctx := &Context{ storage: &storage.Storage{ RootPath: "." } }
	r.HandleFunc("/v1/images/{imageId}/layer", ctx.GetImageLayerHandler).Methods("GET")
	r.HandleFunc("/v1/images/{imageId}/layer", ctx.PutImageLayerHandler).Methods("PUT")
	r.HandleFunc("/v1/images/{imageId}/json", ctx.GetImageJsonHandler).Methods("GET")
	r.HandleFunc("/v1/images/{imageId}/json", ctx.PutImageJsonHandler).Methods("PUT")
	r.HandleFunc("/v1/images/{imageId}/ancestry", ctx.GetImageAncestryHandler).Methods("GET")

	r.HandleFunc("/v1/repositories/{namespace}/{repository}/tags", ctx.GetTagsHandler).Methods("GET")
	r.HandleFunc("/v1/repositories/{namespace}/{repository}/tags/{tag}", ctx.GetTagHandler).Methods("GET")
	r.HandleFunc("/v1/repositories/{namespace}/{repository}/tags/{tag}", ctx.PutTagHandler).Methods("PUT")
	r.HandleFunc("/v1/repositories/{namespace}/{repository}/tags/{tag}", ctx.DeleteTagHandler).Methods("DELETE")
	r.HandleFunc("/v1/repositories/{namespace}/{repository}/", ctx.DeleteRepoHandler).Methods("DELETE")


	// index stuff
	r.HandleFunc("/v1/users", LoginHandler)
	r.HandleFunc("/v1/repositories/{namespace}/{repository}/images", ctx.ListImagesHandler).Methods("GET")
	r.HandleFunc("/v1/repositories/{namespace}/{repository}/images", ctx.PutImageHandler).Methods("PUT")


	http.ListenAndServe(":8080", r)
}
