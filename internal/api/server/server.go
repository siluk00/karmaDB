package server

import (
	"net/http"

	"github.com/gorilla/mux"
)

func NewHTTPServer(addr string) *http.Server {
	r := mux.NewRouter()

	r.HandleFunc("/v1/key/{key}", PutHandler).Methods("PUT")
	r.HandleFunc("/v1/key/{key}", GetHandler).Methods("GET")
	r.HandleFunc("/vq/key/{key}", DeleteHandler).Methods("DELETE")

	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}
