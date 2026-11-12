package main

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed web/index.html
var web embed.FS

func adminPage() http.Handler {
	sub, _ := fs.Sub(web, "web")
	return http.FileServerFS(sub)
}
