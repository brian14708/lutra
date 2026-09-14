// Package web serves the Lutra single-page application built by Vite.
package web

import (
	"io"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

// New returns a handler that serves the static files in fsys, falling back to
// index.html for requests that match no file so client-side routes such as
// /users/1 load on direct visits. fsys is typically os.DirFS of the Vite build
// output (console/dist/client), or an embed.FS to ship the UI inside the binary.
func New(fsys fs.FS) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		// path.Clean keeps the name inside fsys and strips leading slashes.
		name := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
		if name == "" {
			name = "index.html"
		}
		if serveFile(w, r, fsys, name) {
			return
		}
		// Missing hashed assets must remain 404s. Returning index.html here would
		// make the browser report an HTML MIME/module error instead.
		if strings.HasPrefix(name, "assets/") {
			http.NotFound(w, r)
			return
		}
		// No matching file: serve the SPA shell.
		if name != "index.html" && serveFile(w, r, fsys, "index.html") {
			return
		}
		http.NotFound(w, r)
	})
}

// serveFile serves name from fsys with Range and If-Modified-Since support,
// reporting whether name existed as a regular file.
func serveFile(w http.ResponseWriter, r *http.Request, fsys fs.FS, name string) bool {
	f, err := fsys.Open(name)
	if err != nil {
		return false
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil || info.IsDir() {
		return false
	}
	rs, ok := f.(io.ReadSeeker)
	if !ok {
		return false
	}
	// Vite emits content-hashed files under /assets; the shell must always be revalidated.
	if strings.HasPrefix(name, "assets/") {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	} else if name == "index.html" {
		w.Header().Set("Cache-Control", "no-cache")
	}
	http.ServeContent(w, r, info.Name(), info.ModTime(), rs)
	return true
}
