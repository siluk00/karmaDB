package main

import (
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/siluk00/karmaDB/internal/api/server"
)

func main() {
	r := mux.NewRouter()

	r.HandleFunc("/v1/key/{key}", server.PutHandler).Methods("PUT")

	log.Fatal(http.ListenAndServe(":8080", r))

}
