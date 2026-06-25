/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package auth

import (
	"fmt"
	"log/slog"
	"net/http"
)

const pageStyle = `font-family:system-ui,sans-serif;display:flex;` +
	`justify-content:center;align-items:center;height:100vh;margin:0`

const redirectPageTmpl = `<!DOCTYPE html>
<html><head><title>%s</title></head>
<body style="` + pageStyle + `">
<div style="text-align:center"><p>%s</p></div>
<script>window.location.href = %q;</script>
</body></html>`

const autoClosePageTmpl = `<!DOCTYPE html>
<html><head><title>Authentication Successful</title></head>
<body style="` + pageStyle + `">
<div style="text-align:center">
<h2>Authentication successful</h2>
<p id="msg">Completing authentication...</p>
</div>
<script>
(function() {
  window.location.href = %q;
  document.getElementById("msg").textContent =
    "This tab will close automatically.";
  setTimeout(function(){ window.close(); }, 2000);
})();
</script>
</body></html>`

func writeRedirectPage(w http.ResponseWriter, targetURL, message string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if _, err := fmt.Fprintf(w, redirectPageTmpl, message, message, targetURL); err != nil {
		slog.Error("failed to write redirect page", "error", err)
	}
}

func writeAutoClosePage(w http.ResponseWriter, redirectURL string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if _, err := fmt.Fprintf(w, autoClosePageTmpl, redirectURL); err != nil {
		slog.Error("failed to write auto-close page", "error", err)
	}
}
