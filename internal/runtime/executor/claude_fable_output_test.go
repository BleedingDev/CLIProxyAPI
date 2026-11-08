package executor

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	"github.com/tidwall/gjson"
)

// end_turn is protocol metadata, not answer text. Conversely, a model's
// literal "\nend" is valid text and must not be heuristically trimmed.
func TestClaudeFableEndTurnDoesNotCreateOrStripText(t *testing.T) {
	for _, answer := range []string{"PROBE_exact", "PROBE_exact\nend"} {
		t.Run(answer, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				events := []map[string]any{
					{"type": "message_start", "message": map[string]any{"id": "msg_probe", "type": "message", "role": "assistant", "content": []any{}, "model": "claude-fable-5", "usage": map[string]int{"input_tokens": 1, "output_tokens": 0}}},
					{"type": "content_block_start", "index": 0, "content_block": map[string]any{"type": "text", "text": ""}},
					{"type": "content_block_delta", "index": 0, "delta": map[string]any{"type": "text_delta", "text": answer}},
					{"type": "content_block_stop", "index": 0},
					{"type": "message_delta", "delta": map[string]any{"stop_reason": "end_turn", "stop_sequence": nil}, "usage": map[string]int{"output_tokens": 3}},
					{"type": "message_stop"},
				}
				for _, event := range events {
					raw, _ := json.Marshal(event)
					fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event["type"], raw)
				}
				w.(http.Flusher).Flush()
			}))
			defer server.Close()
			exec := NewClaudeExecutor(&config.Config{})
			result, err := exec.ExecuteStream(t.Context(), &cliproxyauth.Auth{Attributes: map[string]string{
				"api_key": "fake-claude-key", "base_url": server.URL,
			}}, cliproxyexecutor.Request{
				Model: "claude-fable-5", Payload: []byte(`{"model":"claude-fable-5","messages":[{"role":"user","content":"reply exactly"}],"stream":true}`),
			}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatClaude, ResponseFormat: sdktranslator.FormatClaude})
			if err != nil {
				t.Fatal(err)
			}
			var text strings.Builder
			for chunk := range result.Chunks {
				if chunk.Err != nil {
					t.Fatal(chunk.Err)
				}
				for _, line := range strings.Split(string(chunk.Payload), "\n") {
					if !strings.HasPrefix(line, "data:") {
						continue
					}
					event := gjson.Parse(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
					if event.Get("delta.type").String() == "text_delta" {
						text.WriteString(event.Get("delta.text").String())
					}
				}
			}
			if text.String() != answer {
				t.Fatalf("upstream text %q became %q", answer, text.String())
			}
		})
	}
}
