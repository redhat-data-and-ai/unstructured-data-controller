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
	"encoding/json"
	"html/template"
	"log/slog"
	"net/http"
)

const pageStyle = `font-family:system-ui,sans-serif;display:flex;` +
	`justify-content:center;align-items:center;height:100vh;margin:0`

// JS redirect + clickable fallback is more reliable than meta-refresh for
// Cursor's cursor:// and http://localhost:8787/callback handoffs.
const redirectPageTmpl = `<!DOCTYPE html>
<html>
<head>
<title>{{.Message}}</title>
</head>
<body style="` + pageStyle + `">
<div style="text-align:center">
<p>{{.Message}}</p>
<p><a id="continue" href="{{.URL}}">Continue</a></p>
</div>
<script>window.location.replace({{.URLJS}});</script>
</body>
</html>`

const callbackPageTmpl = `<!DOCTYPE html>
<html>
<head>
<title>Authentication Successful</title>
</head>
<body style="` + pageStyle + `">
<div style="text-align:center">
<h2>Authentication complete</h2>
<p>Returning to Cursor&hellip;</p>
<p>If Cursor stays on &ldquo;waiting for callback&rdquo;, click:</p>
<p><a id="continue" href="{{.URL}}">Open Cursor callback</a></p>
</div>
<script>window.location.replace({{.URLJS}});</script>
</body>
</html>`

type redirectPageData struct {
	Message string
	// URL is typed template.URL (rather than string) because html/template
	// otherwise sanitizes href values against a scheme allowlist that does
	// NOT include custom schemes like cursor:// — an untyped string would
	// silently render as the inert placeholder "#ZgotmplZ" instead of the
	// real link. Callers must only pass already-validated redirect URIs
	// (see isValidRedirectURI / HandleRegister's scheme checks).
	URL   template.URL
	URLJS template.JS
}

func writeRedirectPage(w http.ResponseWriter, targetURL, message string) {
	writeHTMLTemplate(w, redirectPageTmpl, redirectPageData{
		Message: message,
		URL:     template.URL(targetURL),
		URLJS:   jsString(targetURL),
	})
}

func writeCallbackPage(w http.ResponseWriter, redirectURL string) {
	writeHTMLTemplate(w, callbackPageTmpl, redirectPageData{
		URL:   template.URL(redirectURL),
		URLJS: jsString(redirectURL),
	})
}

func writeHTMLTemplate(w http.ResponseWriter, tmpl string, data redirectPageData) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	t, err := template.New("page").Parse(tmpl)
	if err != nil {
		slog.Error("failed to parse redirect page template", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if err := t.Execute(w, data); err != nil {
		slog.Error("failed to write redirect page", "error", err)
	}
}

// jsString returns s as a safely-quoted JS string literal for embedding
// directly inside a <script> block (avoids the double-escaping bug that
// comes from mixing html/template auto-escaping with manual escaping).
func jsString(s string) template.JS {
	b, err := json.Marshal(s)
	if err != nil {
		return template.JS(`""`)
	}
	return template.JS(b)
}
