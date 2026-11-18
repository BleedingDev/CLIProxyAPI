package executor

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/MoeForge/moego/ojson"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
)

func TestCodexFailureDiagnosticPreservesEnvelope(t *testing.T) {
	long := strings.Repeat("provider detail ", 700)
	event := []byte(fmt.Sprintf(`{"type":"response.failed","trace":{"opaque":[null,true,9007199254740993]},"response":{"id":"resp_full","error":{"code":"context_length_exceeded","type":"invalid_request_error","message":%q,"custom":{"leaf":"retained"}}}}`, long))
	err, _, ok := codexTerminalFailureErr(event)
	if !ok {
		t.Fatal("failure was not recognized")
	}
	err = codexFailureDiagnostic(err, codexFailureDetails{HTTPStatus: 200, Headers: http.Header{"X-Request-Id": {"req_full"}, "Authorization": {"Bearer excluded"}}, LastEvent: "response.failed", Event: event})
	body := []byte(err.Error())
	for path, want := range map[string]string{
		"error.message":                                           long,
		"error.custom.leaf":                                       "retained",
		"error.provider_error.code":                               "context_length_exceeded",
		"error.diagnostics.upstream_request_id":                   "req_full",
		"error.diagnostics.upstream_event.response.error.message": long,
		"trace.opaque.2":                                          "9007199254740993",
	} {
		if got := ojson.GetBytesNoCopy(body, path).String(); got != want {
			t.Errorf("%s = %q, want %q", path, got, want)
		}
	}
	if strings.Contains(err.Error(), "excluded") {
		t.Fatal("diagnostic copied credential headers")
	}
	rawBody := []byte(" {\n\"error\": {\"code\":\"custom\", \"diagnostics\":{\"origin\":\"provider-origin\",\"upstream_body\":\"provider-value\"},\"message\":\"provider message\"},\"arbitrary\":[null,9007199254740993]} \n")
	httpErr := newCodexStatusErrWithHeaders(http.StatusBadGateway, nil, rawBody)
	backup := ojson.Get(httpErr.Error(), "error.diagnostics.upstream_body").String()
	if backup != string(rawBody) || ojson.Get(backup, "error.diagnostics.origin").String() != "provider-origin" || ojson.Get(backup, "error.diagnostics.upstream_body").String() != "provider-value" {
		t.Fatalf("provider field collision lost the exact original body: %q", backup)
	}
	again := codexFailureDiagnostic(httpErr, codexFailureDetails{LastEvent: "error"})
	if ojson.Get(again.Error(), "error.diagnostics.upstream_body").String() != string(rawBody) {
		t.Fatal("enrichment replaced the original body with a proxy envelope")
	}
	incomplete := newCodexEmptyIncompleteStreamError(codexFailureDetails{Event: []byte(`{"type":"response.incomplete","response":{"incomplete_details":{"reason":"provider_specific_reason"}}}`)})
	if got := ojson.Get(incomplete.Error(), "error.diagnostics.incomplete_reason").String(); got != "provider_specific_reason" {
		t.Fatalf("incomplete reason = %q", got)
	}
}

func TestCodexExecutorStreamFailureRetainsEOFMetadata(t *testing.T) {
	for _, buffering := range []bool{false, true} {
		t.Run(fmt.Sprint(buffering), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				conn, rw, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				defer conn.Close()
				_, _ = io.WriteString(rw, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nX-Request-Id: req_eof\r\nContent-Length: 99999\r\n\r\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_eof\"}}\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n")
				_ = rw.Flush()
			}))
			defer server.Close()
			cfg := &config.Config{}
			cfg.Codex.StreamBootstrapBuffering = buffering
			executor := NewCodexExecutor(cfg)
			result, streamErr := executor.ExecuteStream(context.Background(), &cliproxyauth.Auth{Attributes: map[string]string{"base_url": server.URL, "api_key": "test"}}, cliproxyexecutor.Request{Model: "gpt-5.5", Payload: []byte(`{"model":"gpt-5.5","input":"hello"}`)}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai-response"), Stream: true})
			if streamErr == nil {
				for chunk := range result.Chunks {
					if chunk.Err != nil {
						streamErr = chunk.Err
					}
				}
			}
			if streamErr == nil {
				t.Fatal("truncated HTTP body succeeded")
			}
			for path, want := range map[string]string{"origin": "cliproxy.codex", "upstream_http_status": "200", "upstream_request_id": "req_eof", "last_event": "response.output_text.delta", "cause": "unexpected EOF"} {
				if got := ojson.Get(streamErr.Error(), "error.diagnostics."+path).String(); got != want {
					t.Errorf("%s = %q, want %q; err=%v", path, got, want, streamErr)
				}
			}
		})
	}
}

func TestCodexFailureDiagnosticHTTPRejectionMetadata(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, truncated := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%t/truncated=%t", stream, truncated), func(t *testing.T) {
				body := ` {"error":{"code":"provider_failed","message":"upstream rejected","custom":[null,true,9007199254740993]},"vendor":"` + strings.Repeat("raw", 2000) + `"} `
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					w.Header().Set("X-Request-Id", "header_only_request_id")
					w.Header().Set("Traceparent", "00-provider-trace")
					w.Header().Set("Authorization", "Bearer excluded-header-secret")
					w.Header().Set("Set-Cookie", "excluded-cookie-secret")
					if truncated {
						w.Header().Set("Content-Length", "99999")
					}
					w.WriteHeader(http.StatusServiceUnavailable)
					_, _ = io.WriteString(w, body)
				}))
				defer server.Close()
				executor := NewCodexExecutor(&config.Config{})
				auth := &cliproxyauth.Auth{Attributes: map[string]string{"base_url": server.URL, "api_key": "test"}}
				req := cliproxyexecutor.Request{Model: "gpt-5.5", Payload: []byte(`{"model":"gpt-5.5","input":"hello"}`)}
				opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai-response"), Stream: stream}
				var err error
				if stream {
					_, err = executor.ExecuteStream(context.Background(), auth, req, opts)
				} else {
					_, err = executor.Execute(context.Background(), auth, req, opts)
				}
				if err == nil {
					t.Fatal("HTTP rejection succeeded")
				}
				for path, want := range map[string]string{
					"upstream_http_status": "503", "upstream_request_id": "header_only_request_id", "upstream_body": body,
					"upstream_headers.x-request-id.0": "header_only_request_id", "upstream_headers.content-type.0": "application/json", "upstream_headers.traceparent.0": "00-provider-trace",
				} {
					if got := ojson.Get(err.Error(), "error.diagnostics."+path).String(); got != want {
						t.Errorf("%s lost: got=%q want=%q", path, got, want)
					}
				}
				if strings.Contains(err.Error(), "excluded-header-secret") || strings.Contains(err.Error(), "excluded-cookie-secret") {
					t.Fatal("diagnostic retained credential response headers")
				}
				if truncated && ojson.Get(err.Error(), "error.diagnostics.cause").String() != "unexpected EOF" {
					t.Fatalf("HTTP rejection lost body read failure: %v", err)
				}
			})
		}
	}
}
