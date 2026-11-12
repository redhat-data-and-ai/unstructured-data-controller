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
	"html/template"
	"log/slog"
	"net/http"
)

const pageStyle = `font-family:system-ui,sans-serif;display:flex;` +
	`justify-content:center;align-items:center;height:100vh;margin:0`

// Parsed at init so a malformed template fails the build's tests rather than a
// live request. html/template escapes TargetURL per context, so a redirect_uri
// cannot break out of the attribute it is interpolated into.
var redirectPageTmpl = template.Must(template.New("redirect").Parse(`<!DOCTYPE html>
<html>
<head>
<title>{{.Message}}</title>
<meta http-equiv="refresh" content="0; url={{.TargetURL}}" />
</head>
<body style="` + pageStyle + `">
<div style="text-align:center"><p>{{.Message}}</p></div>
</body>
</html>`))

var callbackPageTmpl = template.Must(template.New("callback").Parse(`<!DOCTYPE html>
<html>
<head>
<title>Authentication Successful</title>
<meta http-equiv="refresh" content="0; url={{.TargetURL}}" />
</head>
<body style="` + pageStyle + `">
<div style="text-align:center">
<h2>Authentication complete</h2>
<p>You may now close this window.</p>
</div>
</body>
</html>`))

type pageData struct {
	TargetURL string
	Message   string
}

func writeRedirectPage(w http.ResponseWriter, targetURL, message string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := redirectPageTmpl.Execute(w, pageData{TargetURL: targetURL, Message: message}); err != nil {
		slog.Error("failed to write redirect page", "error", err)
	}
}

func writeCallbackPage(w http.ResponseWriter, redirectURL string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := callbackPageTmpl.Execute(w, pageData{TargetURL: redirectURL}); err != nil {
		slog.Error("failed to write callback page", "error", err)
	}
}
