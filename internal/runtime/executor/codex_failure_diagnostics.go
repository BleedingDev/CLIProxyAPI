package executor

import (
	"net/http"
	"strings"

	"github.com/MoeForge/moego/ojson"
)

type codexFailureDetails struct {
	HTTPStatus int
	Headers    http.Header
	LastEvent  string
	Event      []byte
	Body       []byte
	Cause      error
}

func codexFailureDiagnostic(err statusErr, details codexFailureDetails) statusErr {
	body := []byte(err.msg)
	if !ojson.ValidBytes(body) || !ojson.GetBytesNoCopy(body, "error").IsObject() {
		doc := ojson.NewDoc([]byte(`{"error":{}}`))
		_ = doc.Set("error.message", err.Error())
		_ = doc.Set("error.upstream_body", err.msg)
		body = doc.Bytes()
	}
	doc := ojson.NewDoc(body)
	upstreamBody := details.Body
	if len(details.Event) > 0 {
		upstreamBody = details.Event
	}
	if len(upstreamBody) == 0 && !doc.Get("error.diagnostics.upstream_body").Exists() {
		upstreamBody = []byte(err.msg)
	}
	if len(upstreamBody) > 0 {
		_ = doc.Set("error.diagnostics.upstream_body", string(upstreamBody))
	}
	_ = doc.Set("error.diagnostics.origin", "cliproxy.codex")
	if details.HTTPStatus > 0 {
		_ = doc.Set("error.diagnostics.upstream_http_status", details.HTTPStatus)
	}
	for _, name := range []string{"X-Request-Id", "Request-Id", "Openai-Request-Id", "X-Amzn-Requestid", "Cf-Ray"} {
		if id := strings.TrimSpace(details.Headers.Get(name)); id != "" {
			_ = doc.Set("error.diagnostics.upstream_request_id", id)
			break
		}
	}
	if details.LastEvent != "" {
		_ = doc.Set("error.diagnostics.last_event", details.LastEvent)
	}
	if len(details.Event) > 0 && ojson.ValidBytes(details.Event) {
		_ = doc.SetRawBytes("error.diagnostics.upstream_event", details.Event)
		if reason := ojson.GetBytesNoCopy(details.Event, "response.incomplete_details.reason"); reason.Exists() {
			_ = doc.SetRawBytes("error.diagnostics.incomplete_reason", []byte(reason.Raw))
		}
	}
	if details.Cause != nil {
		_ = doc.Set("error.diagnostics.cause", details.Cause.Error())
	}
	err.msg = string(doc.Bytes())
	return err
}
