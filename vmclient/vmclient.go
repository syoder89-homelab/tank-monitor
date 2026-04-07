package vmclient

import (
	"fmt"
	"time"
	"bytes"
	"io"
	"net/http"
	"net/url"
	"compress/gzip"
	"regexp"
	"strings"
	"log"
	"github.com/VictoriaMetrics/metrics"
)

func Push(pushURL string, timeout time.Duration, extraLabels string, pushProcessMetrics bool) error {
	writeMetrics := func(w io.Writer) {
		metrics.WritePrometheus(w, pushProcessMetrics)
	}
	return PushExt(pushURL, timeout, extraLabels, writeMetrics)
}

var identRegexp = regexp.MustCompile("^[a-zA-Z_:.][a-zA-Z0-9_:.]*$")

func validateTags(s string) error {
	if len(s) == 0 {
		return nil
	}
	for {
		n := strings.IndexByte(s, '=')
		if n < 0 {
			return fmt.Errorf("missing `=` after %q", s)
		}
		ident := s[:n]
		s = s[n+1:]
		if err := validateIdent(ident); err != nil {
			return err
		}
		if len(s) == 0 || s[0] != '"' {
			return fmt.Errorf("missing starting `\"` for %q value; tail=%q", ident, s)
		}
		s = s[1:]
	again:
		n = strings.IndexByte(s, '"')
		if n < 0 {
			return fmt.Errorf("missing trailing `\"` for %q value; tail=%q", ident, s)
		}
		m := n
		for m > 0 && s[m-1] == '\\' {
			m--
		}
		if (n-m)%2 == 1 {
			s = s[n+1:]
			goto again
		}
		s = s[n+1:]
		if len(s) == 0 {
			return nil
		}
		if !strings.HasPrefix(s, ",") {
			return fmt.Errorf("missing `,` after %q value; tail=%q", ident, s)
		}
		s = skipSpace(s[1:])
	}
}

func skipSpace(s string) string {
	for len(s) > 0 && s[0] == ' ' {
		s = s[1:]
	}
	return s
}

func validateIdent(s string) error {
	if !identRegexp.MatchString(s) {
		return fmt.Errorf("invalid identifier %q", s)
	}
	return nil
}

func addExtraLabels(dst, src []byte, extraLabels string) []byte {
	for len(src) > 0 {
		var line []byte
		n := bytes.IndexByte(src, '\n')
		if n >= 0 {
			line = src[:n]
			src = src[n+1:]
		} else {
			line = src
			src = nil
		}
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			// Skip empy lines
			continue
		}
		if bytes.HasPrefix(line, bashBytes) {
			// Copy comments as is
			dst = append(dst, line...)
			dst = append(dst, '\n')
			continue
		}
		n = bytes.IndexByte(line, '{')
		if n >= 0 {
			dst = append(dst, line[:n+1]...)
			dst = append(dst, extraLabels...)
			dst = append(dst, ',')
			dst = append(dst, line[n+1:]...)
		} else {
			n = bytes.LastIndexByte(line, ' ')
			if n < 0 {
				panic(fmt.Errorf("BUG: missing whitespace between metric name and metric value in Prometheus text exposition line %q", line))
			}
			dst = append(dst, line[:n]...)
			dst = append(dst, '{')
			dst = append(dst, extraLabels...)
			dst = append(dst, '}')
			dst = append(dst, line[n:]...)
		}
		dst = append(dst, '\n')
	}
	return dst
}

var bashBytes = []byte("#")

func PushExt(pushURL string, timeout time.Duration, extraLabels string, writeMetrics func(w io.Writer)) error {
	if err := validateTags(extraLabels); err != nil {
		return fmt.Errorf("invalid extraLabels=%q: %w", extraLabels, err)
	}
	pu, err := url.Parse(pushURL)
	if err != nil {
		return fmt.Errorf("cannot parse pushURL=%q: %w", pushURL, err)
	}
	if pu.Scheme != "http" && pu.Scheme != "https" {
		return fmt.Errorf("unsupported scheme in pushURL=%q; expecting 'http' or 'https'", pushURL)
	}
	if pu.Host == "" {
		return fmt.Errorf("missing host in pushURL=%q", pushURL)
	}
	pushURLRedacted := pu.Redacted()
	c := &http.Client{
		Timeout: timeout,
	}
	go func() {
		var bb bytes.Buffer
		var tmpBuf []byte
		zw := gzip.NewWriter(&bb)
		bb.Reset()
		writeMetrics(&bb)
		if len(extraLabels) > 0 {
			tmpBuf = addExtraLabels(tmpBuf[:0], bb.Bytes(), extraLabels)
			bb.Reset()
			if _, err := bb.Write(tmpBuf); err != nil {
				panic(fmt.Errorf("BUG: cannot write %d bytes to bytes.Buffer: %s", len(tmpBuf), err))
			}
		}
		tmpBuf = append(tmpBuf[:0], bb.Bytes()...)
		bb.Reset()
		zw.Reset(&bb)
		if _, err := zw.Write(tmpBuf); err != nil {
			panic(fmt.Errorf("BUG: cannot write %d bytes to gzip writer: %s", len(tmpBuf), err))
		}
		if err := zw.Close(); err != nil {
			panic(fmt.Errorf("BUG: cannot flush metrics to gzip writer: %s", err))
		}
		req, err := http.NewRequest("POST", pushURL, &bb)
		if err != nil {
			panic(fmt.Errorf("BUG: metrics.push: cannot initialize request for metrics push to %q: %w", pushURLRedacted, err))
		}
		req.Header.Set("Content-Type", "text/plain")
		req.Header.Set("Content-Encoding", "gzip")
		resp, err := c.Do(req)
		if err != nil {
			log.Printf("ERROR: metrics.push: cannot push metrics to %q: %s", pushURLRedacted, err)
			return
		}
		if resp.StatusCode/100 != 2 {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			log.Printf("ERROR: metrics.push: unexpected status code in response from %q: %d; expecting 2xx; response body: %q",
				pushURLRedacted, resp.StatusCode, body)
			return
		}
		_ = resp.Body.Close()
	}()
	return nil
}
