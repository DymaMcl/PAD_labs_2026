package main

import (
	"embed"
	"io/fs"
	"net/http"

	"github.com/DymaMcl/PAD_labs_2026/laborator_1/broker"
)

//go:embed web/index.html
var web embed.FS

// mountDashboard serveste interfata web (inclusa in binar) la "/".
func mountDashboard(s *broker.Server) {
	sub, _ := fs.Sub(web, "web")
	s.Handle("GET /{$}", http.FileServerFS(sub))
}
