package gateway

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	schedulerv1 "agentenv/services/api/proto"
)

// buildScheduleHint inspects the incoming request and produces a structured
// scheduling hint. Requests that do not map to a known hint type return a nil
// hint. For cold-sandbox creation the request body is parsed to extract
// structured fields and then restored so the full body remains available for
// the upstream request.
// It only returns error when it's a fatal error and the request cannot be proceeded.
func buildScheduleHint(r *http.Request) (*schedulerv1.ScheduleRequestHint, error) {
	if r.Method != http.MethodPost {
		return nil, nil
	}
	switch strings.TrimRight(r.URL.Path, "/") {
	case "/sandboxes-cold":
		body, err := captureRequestBody(r)
		if err != nil {
			return nil, err
		}
		return &schedulerv1.ScheduleRequestHint{
			Kind: &schedulerv1.ScheduleRequestHint_NewColdSandbox{
				NewColdSandbox: parseNewColdSandboxHint(body),
			},
		}, nil
	case "/sandboxes":
		body, err := captureRequestBody(r)
		if err != nil {
			return nil, err
		}
		return &schedulerv1.ScheduleRequestHint{
			Kind: &schedulerv1.ScheduleRequestHint_NewSandbox{
				NewSandbox: parseNewSandboxHint(body),
			},
		}, nil
	default:
		return nil, nil
	}
}

// maxHintBodyBytes bounds how much of a request body the gateway buffers in
// memory while extracting a scheduling hint. Cold-sandbox creation bodies are
// small, so anything larger is assumed not worth inspecting. Keeping a bound
// here matters because hint extraction runs before upstream authentication;
// without it an unauthenticated client could force the gateway to buffer an
// arbitrarily large body.
const maxHintBodyBytes = 64 * 1024

// captureRequestBody buffers up to maxHintBodyBytes of the request body so a
// scheduling hint can be extracted, then restores r.Body so the full body
// remains available for the upstream request. If the body exceeds the budget,
// the buffered prefix is stitched back in front of the unread remainder (no
// full buffering) and a nil body is returned so the caller skips hint
// extraction.
func captureRequestBody(r *http.Request) ([]byte, error) {
	if r.Body == nil || r.Body == http.NoBody {
		return nil, nil
	}
	orig := r.Body
	buf, err := io.ReadAll(io.LimitReader(orig, maxHintBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(buf)) > maxHintBodyBytes {
		// Too large to inspect: restore the full stream without buffering the
		// remainder and skip hint extraction.
		r.Body = &prefixedBody{Reader: io.MultiReader(bytes.NewReader(buf), orig), closer: orig}
		return nil, nil
	}
	_ = orig.Close()
	r.Body = io.NopCloser(bytes.NewReader(buf))
	r.ContentLength = int64(len(buf))
	return buf, nil
}

// prefixedBody re-presents an already-partially-read body as a single
// ReadCloser: the buffered prefix followed by the unread remainder, while
// closing the underlying body.
type prefixedBody struct {
	io.Reader
	closer io.Closer
}

func (b *prefixedBody) Close() error { return b.closer.Close() }

// newColdSandboxBody mirrors the subset of NewColdSandbox
// (src/api/openapi.yml) that is relevant for scheduling.
type newColdSandboxBody struct {
	Image          string            `json:"image"`
	CPUCount       uint32            `json:"cpuCount"`
	MemoryMB       uint64            `json:"memoryMB"`
	Metadata       map[string]string `json:"metadata"`
	AttachedDrives []struct {
		Source struct {
			Image string `json:"image"`
		} `json:"source"`
	} `json:"attachedDrives"`
}

// parseNewColdSandboxHint extracts the structured cold-sandbox hint from the
// request body. Malformed or partial bodies yield a best-effort hint rather
// than an error, since scheduling hints are advisory.
func parseNewColdSandboxHint(body []byte) *schedulerv1.NewColdSandboxHint {
	hint := &schedulerv1.NewColdSandboxHint{}
	if len(body) == 0 {
		return hint
	}
	var parsed newColdSandboxBody
	if err := json.Unmarshal(body, &parsed); err != nil {
		return hint
	}
	hint.CpuCount = parsed.CPUCount
	hint.MemoryMb = parsed.MemoryMB
	hint.Metadata = parsed.Metadata
	if parsed.Image != "" {
		hint.Images = append(hint.Images, parsed.Image)
	}
	for _, drive := range parsed.AttachedDrives {
		if drive.Source.Image != "" {
			hint.Images = append(hint.Images, drive.Source.Image)
		}
	}
	return hint
}

// newSandboxBody mirrors the subset of NewSandbox (src/api/openapi.yml) that is
// relevant for scheduling.
type newSandboxBody struct {
	Metadata map[string]string `json:"metadata"`
}

// parseNewSandboxHint extracts the structured sandbox hint from the request
// body. Malformed or partial bodies yield a best-effort hint rather than an
// error, since scheduling hints are advisory.
func parseNewSandboxHint(body []byte) *schedulerv1.NewSandboxHint {
	hint := &schedulerv1.NewSandboxHint{}
	if len(body) == 0 {
		return hint
	}
	var parsed newSandboxBody
	if err := json.Unmarshal(body, &parsed); err != nil {
		return hint
	}
	hint.Metadata = parsed.Metadata
	return hint
}

// maxReplayBodyBytes bounds how much of a request body the gateway holds so the
// request can be sent to a second node. Sized for control-plane calls like
// resume, whose bodies are a handful of fields.
const maxReplayBodyBytes = 64 << 10

// captureReplayBody buffers a request body so the request can be forwarded
// twice. It reports whether a replay is possible: a body larger than the bound
// is left as an intact stream and must not be replayed, since only a truncated
// prefix was ever held.
func captureReplayBody(r *http.Request) ([]byte, bool, error) {
	if r.Body == nil || r.Body == http.NoBody {
		return nil, true, nil
	}

	orig := r.Body
	buf, err := io.ReadAll(io.LimitReader(orig, maxReplayBodyBytes+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(buf)) > maxReplayBodyBytes {
		r.Body = &prefixedBody{Reader: io.MultiReader(bytes.NewReader(buf), orig), closer: orig}
		return nil, false, nil
	}
	_ = orig.Close()
	restoreReplayBody(r, buf)

	return buf, true, nil
}

// restoreReplayBody re-presents a buffered body as a fresh readable stream, so
// a request whose body was already consumed by one forward can be sent again.
func restoreReplayBody(r *http.Request, body []byte) {
	if len(body) == 0 {
		r.Body = http.NoBody
		r.ContentLength = 0
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
}
