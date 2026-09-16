// Copyright 2023 - MinIO, Inc. All rights reserved.
// Use of this source code is governed by the AGPLv3
// license that can be found in the LICENSE file.

package https

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"
)

// LoadBalancer is an http.RoundTripper that implements client-side
// load balancing.
//
// A LoadBalancer distributes requests uniformly across a list of hosts.
// Clients should use the Host or URL method to construct a request to
// one of these hosts.
//
// If a request to one of these hosts fails, the LoadBalancer retries
// the request with different hosts until either the request succeeds
// or there are no hosts remaining resp. the retry limit is reached.
// Hosts for which requests fail are suspended and no longer selected
// for subsequent requests.
//
// A suspended host is re-admitted once a Probe of that host succeeds.
// Without a Probe, it is re-admitted after Timeout elapsed.
type LoadBalancer struct {
	// Underlying RoundTripper used to send requests.
	http.RoundTripper

	// List of hosts the requests are distributed over.
	//
	// If a request fails and its URL host is not part of
	// this list then the LoadBalancer will not retry the
	// request.
	Hosts []string

	// Timeout controls how long a host is suspended if a
	// request to this host fails. If 0, defaults to 30
	// seconds.
	//
	// It is only used if Probe is nil.
	Timeout time.Duration

	// Probe reports whether a host is able to serve requests.
	// It should return a nil error if, and only if, the host
	// responded.
	//
	// If Probe is not nil, a suspended host is only re-admitted
	// once a Probe of that host succeeds. An unreachable host
	// therefore stays suspended instead of being re-admitted on
	// a timer while it is still unreachable.
	//
	// Probe must not send its requests through the LoadBalancer
	// itself since the LoadBalancer does not select suspended
	// hosts.
	Probe func(ctx context.Context, host string) error

	// ProbeInterval is the delay between two rounds of probing
	// the suspended hosts. If 0, defaults to 5 seconds. A probe
	// is canceled once ProbeInterval elapsed.
	//
	// It is only used if Probe is not nil.
	ProbeInterval time.Duration

	// Retry specifies how often the LoadBalancer retries
	// a request with different hosts bef
	Retry int

	mu      sync.RWMutex
	timeout map[string]time.Time
	probing bool
}

// URL returns an URL string with the next host and the provided
// path elements joined to the existing path of base and the
// resulting path cleaned of any ./ or ../ elements.
//
// The scheme of the returned URL is "https://".
// The second return value is the URL's host.
func (lb *LoadBalancer) URL(elems ...string) (string, string, error) {
	host, err := lb.Host()
	if err != nil {
		return "", host, err
	}

	const Scheme = "https://"
	if !strings.HasPrefix(host, Scheme) {
		host = Scheme + host
	}
	url, err := url.JoinPath(host, elems...)
	return url, host, err
}

// Host returns the next host to send requests to. It
// prefers hosts that aren't currently suspended.
//
// It returns an error if the list of hosts is empty
// but not if there are no non-suspended hosts.
func (lb *LoadBalancer) Host() (string, error) {
	switch len(lb.Hosts) {
	case 0:
		return "", errors.New("https: no hosts provided")
	case 1:
		return lb.Hosts[0], nil
	default:
		r := rand.Intn(len(lb.Hosts))

		lb.mu.RLock()
		defer lb.mu.RUnlock()

		now := time.Now()
		for i := 0; i < len(lb.Hosts); i++ {
			if !lb.isSuspended(lb.Hosts[r], now) {
				return lb.Hosts[r], nil
			}
			r = (r + 1) % len(lb.Hosts)
		}
		return lb.Hosts[r], nil
	}
}

// RoundTrip executes the HTTP request and returns the corresponding
// response on success.
//
// If the request execution fails with a retryable error, RoundTrip
// retries the request with other hosts for which requests have
// succeeded before. It stops retrying once the request succeeds,
// there are no more non-suspended hosts remaining or the retry limit
// is reached.
// Hosts, for which requests fail, are suspended and no longer selected
// for subsequent requests or retries.
func (lb *LoadBalancer) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := lb.RoundTripper.RoundTrip(req)
	if err != nil && isRetryable(req.Context(), err) {
		r := slices.Index(lb.Hosts, req.URL.Host)
		if r < 0 {
			return resp, err
		}
		lb.suspend(req.URL.Host)

		now := time.Now()
		for i := 1; i < len(lb.Hosts); i++ {
			r = (r + 1) % len(lb.Hosts)

			lb.mu.RLock()
			suspended := lb.isSuspended(lb.Hosts[r], now)
			lb.mu.RUnlock()

			if suspended {
				continue
			}
			closeResponseBody(resp)

			req.URL.Host = lb.Hosts[r]
			resp, err = lb.RoundTripper.RoundTrip(req)
			if err == nil || !isRetryable(req.Context(), err) {
				return resp, err
			}
			lb.suspend(req.URL.Host)
		}
	}
	return resp, err
}

// suspend excludes host from host selection. It starts probing the
// suspended hosts if lb re-admits hosts based on Probe and no probing
// is in progress.
func (lb *LoadBalancer) suspend(host string) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if lb.timeout == nil {
		lb.timeout = map[string]time.Time{}
	}
	lb.timeout[host] = time.Now()

	if lb.Probe != nil && !lb.probing {
		lb.probing = true
		go lb.probeHosts()
	}
}

// resume re-admits host to host selection.
func (lb *LoadBalancer) resume(host string) {
	lb.mu.Lock()
	delete(lb.timeout, host)
	lb.mu.Unlock()
}

// isSuspended reports whether host is currently excluded from host
// selection. Callers must hold lb.mu.
func (lb *LoadBalancer) isSuspended(host string, now time.Time) bool {
	t, ok := lb.timeout[host]
	if !ok {
		return false
	}
	if lb.Probe != nil {
		return true
	}
	return now.Sub(t) <= timeout(lb.Timeout)
}

// probeHosts probes all suspended hosts, once per ProbeInterval, and
// re-admits those that respond. It returns once no host is suspended
// anymore. A new probeHosts is started by the next suspend.
func (lb *LoadBalancer) probeHosts() {
	interval := probeInterval(lb.ProbeInterval)
	for {
		hosts := lb.suspendedHosts()
		if len(hosts) == 0 {
			return
		}

		var wg sync.WaitGroup
		for _, host := range hosts {
			wg.Add(1)
			go func() {
				defer wg.Done()

				ctx, cancel := context.WithTimeout(context.Background(), interval)
				defer cancel()

				if lb.Probe(ctx, host) == nil {
					lb.resume(host)
				}
			}()
		}
		wg.Wait()

		time.Sleep(interval)
	}
}

// suspendedHosts returns the currently suspended hosts. It stops
// probing if no host is suspended, such that the emptiness check
// and the hand-off to the next suspend happen atomically.
func (lb *LoadBalancer) suspendedHosts() []string {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if len(lb.timeout) == 0 {
		lb.probing = false
		return nil
	}

	hosts := make([]string, 0, len(lb.timeout))
	for host := range lb.timeout {
		hosts = append(hosts, host)
	}
	return hosts
}

// timeout returns how long a host stays suspended when no Probe decides
// about it, defaulting to 30 seconds.
func timeout(d time.Duration) time.Duration {
	if d <= 0 {
		return 30 * time.Second
	}
	return d
}

// probeInterval returns the delay between two probe rounds, defaulting
// to 5 seconds.
func probeInterval(d time.Duration) time.Duration {
	if d <= 0 {
		return 5 * time.Second
	}
	return d
}

// isRetryable reports whether a request that failed with err should be
// retried with a different host, and therefore whether the host it was
// sent to should be suspended.
func isRetryable(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}

	// A dial or I/O timeout also matches context.DeadlineExceeded since
	// net.Dialer implements its Timeout with a context deadline. Only an
	// expired request context tells the two apart: the caller is gone, so
	// no retry can succeed and the host is not at fault.
	if ctx.Err() != nil {
		return false
	}
	return !errors.Is(err, context.Canceled)
}

// closeResponseBody drains and closes the body of a response that is
// being discarded, so that its connection can be reused.
func closeResponseBody(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}

	const maxBodySlurpSize = 2 << 10
	if resp.ContentLength == -1 || resp.ContentLength <= maxBodySlurpSize {
		io.CopyN(io.Discard, resp.Body, maxBodySlurpSize)
	}
	resp.Body.Close()
}
