package repocache

import (
	"context"
	"encoding/base64"
	"errors"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/githubapp"
)

type remote struct {
	url    string
	config []string
}

func normalizeURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Opaque != "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Hostname() == "" || strings.ContainsAny(raw, "\x00\r\n\\") {
		return nil, errors.New("repository URL must be credential-free HTTPS without query or fragment")
	}
	host := strings.ToLower(u.Hostname())
	if strings.HasSuffix(host, ".") || strings.ContainsAny(host, "%* \t") {
		return nil, errors.New("repository host is invalid")
	}
	for _, r := range host {
		if r > 127 {
			return nil, errors.New("repository host must use ASCII")
		}
	}
	if _, err := netip.ParseAddr(host); err != nil {
		// libcurl accepts legacy integer/octal IPv4 spellings that a DNS
		// resolver may interpret differently. Only canonical IPs are allowed.
		labels := strings.Split(host, ".")
		last := labels[len(labels)-1]
		if _, err := strconv.ParseUint(last, 0, 64); err == nil {
			return nil, errors.New("repository host uses a noncanonical IP spelling")
		}
	}
	port := u.Port()
	if port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return nil, errors.New("repository port is invalid")
		}
		if n == 443 {
			port = ""
		}
	}
	u.Host = host
	if strings.Contains(host, ":") {
		u.Host = "[" + host + "]"
	}
	if port != "" {
		u.Host = net.JoinHostPort(host, port)
	}
	if u.Path == "" {
		u.Path = "/"
	}
	return u, nil
}

// deniedAddress includes non-global ranges that IsGlobalUnicast deliberately accepts.
func deniedAddress(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return true
	}
	for _, raw := range []string{"0.0.0.0/8", "100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "240.0.0.0/4", "64:ff9b::/96", "64:ff9b:1::/48", "100::/64", "2001::/23", "2002::/16", "3fff::/20"} {
		if netip.MustParsePrefix(raw).Contains(ip) {
			return true
		}
	}
	return false
}

func (m *Manager) resolveRemote(ctx context.Context, u *url.URL, token githubapp.Token) (remote, error) {
	host := u.Hostname()
	var addresses []netip.Addr
	if ip, err := netip.ParseAddr(host); err == nil {
		addresses = []netip.Addr{ip}
	} else {
		var err error
		addresses, err = m.lookupIP(ctx, "ip", host)
		if err != nil {
			return remote{}, errors.New("resolve Git remote failed")
		}
	}
	if len(addresses) == 0 {
		return remote{}, errors.New("Git remote has no addresses")
	}
	for _, ip := range addresses {
		if deniedAddress(ip.Unmap()) {
			return remote{}, errors.New("Git remote address is outside the allowed network policy")
		}
	}
	port := u.Port()
	if port == "" {
		port = "443"
	}
	ip := addresses[0].Unmap().String()
	if strings.Contains(ip, ":") {
		ip = "[" + ip + "]"
	}
	r := remote{url: u.String()}
	if _, err := netip.ParseAddr(host); err != nil {
		r.config = []string{"http.curloptResolve=" + host + ":" + port + ":" + ip}
	}
	if token.Value != "" {
		if host != "github.com" || !token.ExpiresAt.After(time.Now()) {
			return remote{}, errors.New("GitHub credential is not valid for this Git remote")
		}
		credential := base64.StdEncoding.EncodeToString([]byte("x-access-token:" + token.Value))
		r.config = append(r.config, "http.https://github.com/.extraHeader=Authorization: Basic "+credential)
	}
	return r, nil
}
