package main

import (
	"github.com/gorilla/mux"
	"github.com/georgebashi/docker_simpleregistry/storage"
	"net/http"
	"fmt"
	"io"
	"strings"
	"encoding/json"
	"crypto/sha256"
)

func PingHandler (w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "true") // lolwut
}

func HomeHandler (w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "docker-registry server (docker_simpleregistry)") // lolwut
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
	//imageId := mux.Vars(r)["imageId"]
	decoder := json.NewDecoder(r.Body)
	var data map[string]string
	if err := decoder.Decode(&data); err != nil {
		sendError(400, "Invalid JSON", w)
		return
	}

	if _, ok := data["id"]; !ok {
		sendError(400, "Missing Key `id' in JSON", w)
		return
	}

	checksum, ok := r.Header.Get("X-Docker-Checksum")
	if !ok {
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

	//r.HandleFunc("/v1/repositories/{namespace}/{repository}/tags", GetTagsHandler).Methods("GET")
	//r.HandleFunc("/v1/repositories/{namespace}/{repository}/tags/{tag}", PutTagsHandler).Methods("PUT")
	//r.HandleFunc("/v1/repositories/{namespace}/{repository}/tags/{tag}", DeleteTagsHandler).Methods("DELETE")


	// index stuff
	//r.HandleFunc("/v1/users", LoginHandler)
	//r.HandleFunc("/v1/repositories/{namespace}/{repository}/images", ListImagesHandler).Methods("GET")
	//r.HandleFunc("/v1/repositories/{namespace}/{repository}/images", PutImageHandler).Methods("PUT")


	http.ListenAndServe(":8080", r)
}
