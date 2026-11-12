package accounts

import (
	"encoding/json"
	"net/http"
)

// AdminHandler expune starea serviciului prin HTTP/JSON (observabilitate si
// injectare de defecte). NU face parte din contractul pentru clienti.
//
//	GET /admin/state    solduri, ultimele operatii, contoare, defecte
//	PUT /admin/faults   seteaza defectele injectate
func (s *Server) AdminHandler() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/state", func(w http.ResponseWriter, _ *http.Request) {
		bal, ops := s.Store.Snapshot(60)
		writeJSON(w, http.StatusOK, map[string]any{
			"balances": bal, "operations": ops, "stats": s.Stats(), "faults": s.Faults(),
			"v2_enabled": !s.opts.DisableV2,
		})
	})
	mux.HandleFunc("PUT /admin/faults", func(w http.ResponseWriter, r *http.Request) {
		var f Faults
		if err := json.NewDecoder(r.Body).Decode(&f); err != nil || f.DropResponseRate < 0 || f.DropResponseRate > 1 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "faults invalide"})
			return
		}
		s.SetFaults(f)
		writeJSON(w, http.StatusOK, f)
	})
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
