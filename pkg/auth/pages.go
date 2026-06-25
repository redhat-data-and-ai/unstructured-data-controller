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
<html>
<head>
<title>%s</title>
<meta http-equiv="refresh" content="0; url=%s" />
</head>
<body style="` + pageStyle + `">
<div style="text-align:center"><p>%s</p></div>
</body>
</html>`

const callbackPageTmpl = `<!DOCTYPE html>
<html>
<head>
<title>Authentication Successful</title>
<meta http-equiv="refresh" content="0; url=%s" />
</head>
<body style="` + pageStyle + `">
<div style="text-align:center">
<h2>Authentication complete</h2>
<p>You may now close this window.</p>
</div>
</body>
</html>`

func writeRedirectPage(w http.ResponseWriter, targetURL, message string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if _, err := fmt.Fprintf(w, redirectPageTmpl, message, targetURL, message); err != nil {
		slog.Error("failed to write redirect page", "error", err)
	}
}

func writeCallbackPage(w http.ResponseWriter, redirectURL string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if _, err := fmt.Fprintf(w, callbackPageTmpl, redirectURL); err != nil {
		slog.Error("failed to write callback page", "error", err)
	}
}
