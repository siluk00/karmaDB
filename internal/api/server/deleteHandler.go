package server

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/siluk00/karmaDB/internal/storage"
)

func DeleteHandler(w http.ResponseWriter, r *http.Request) {
	key, ok := mux.Vars(r)["key"]
	if !ok {
		http.Error(w, "server needs key to delete", http.StatusBadRequest)
	}

	storage.Delete(key)

	w.WriteHeader(http.StatusNoContent)
}
