package kubernetes

import (
	"errors"
	"io"
	"net/http"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/httpstream"
	streamprotocol "k8s.io/apimachinery/pkg/util/remotecommand"
	"k8s.io/client-go/transport/spdy"
)

// The v4 server writes a JSON Status even for exit zero. client-go also accepts
// an empty status-stream EOF for older protocols, which would turn a severed
// connection into success. Keep its stream pumps and decoder, but require the
// positive terminal message promised by the negotiated v4 protocol.
type statusUpgrader struct{ delegate spdy.Upgrader }

func (u statusUpgrader) NewConnection(response *http.Response) (httpstream.Connection, error) {
	connection, err := u.delegate.NewConnection(response)
	if err != nil {
		return nil, err
	}
	if response.Header.Get(httpstream.HeaderProtocolVersion) != streamprotocol.StreamProtocolV4Name {
		_ = connection.Close()
		return nil, errors.New("task exec requires the v4 terminal-status protocol")
	}
	return statusConnection{Connection: connection}, nil
}

type statusConnection struct{ httpstream.Connection }

func (c statusConnection) CreateStream(headers http.Header) (httpstream.Stream, error) {
	stream, err := c.Connection.CreateStream(headers)
	if err != nil {
		return nil, err
	}
	if headers.Get(corev1.StreamType) == corev1.StreamTypeError {
		return &terminalStream{Stream: stream}, nil
	}
	return stream, nil
}

type terminalStream struct {
	httpstream.Stream
	received bool
}

func (s *terminalStream) Read(data []byte) (int, error) {
	n, err := s.Stream.Read(data)
	if n > 0 {
		s.received = true
	}
	if errors.Is(err, io.EOF) && !s.received {
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}
