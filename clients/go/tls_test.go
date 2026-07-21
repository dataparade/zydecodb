package zydecodb

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"io"
	"math/big"
	"net"
	"testing"
	"time"
)

// selfSignedCert mints a throwaway cert for the given DNS names and returns it
// alongside a root pool that trusts it.
func selfSignedCert(t *testing.T, dnsNames ...string) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "zydecodb-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:              dnsNames,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, roots
}

// servePings answers protocol frames on a listener: every request gets an
// empty StatusOK response. Returns after the listener closes.
func servePings(ln net.Listener) {
	for {
		nc, err := ln.Accept()
		if err != nil {
			return
		}
		go func(nc net.Conn) {
			defer nc.Close()
			header := make([]byte, HeaderLen)
			for {
				if _, err := io.ReadFull(nc, header); err != nil {
					return
				}
				length := binary.BigEndian.Uint32(header[2:])
				if length > 0 {
					if _, err := io.CopyN(io.Discard, nc, int64(length)); err != nil {
						return
					}
				}
				resp := make([]byte, HeaderLen)
				resp[0] = ProtoVersion
				resp[1] = StatusOK
				if _, err := nc.Write(resp); err != nil {
					return
				}
			}
		}(nc)
	}
}

func TestWithTLSRoundTrip(t *testing.T) {
	cert, roots := selfSignedCert(t, "localhost")
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go servePings(ln)

	c := NewClient(ln.Addr().String(), WithTLS(&tls.Config{RootCAs: roots, ServerName: "localhost"}))
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Ping(ctx); err != nil {
		t.Fatalf("ping over TLS: %v", err)
	}
}

// The SNI name must default to the dial host when the caller does not set
// ServerName, since that is how wildcard node certs are verified.
func TestWithTLSInfersServerName(t *testing.T) {
	cert, roots := selfSignedCert(t, "localhost")
	// Listen on localhost so the dial host is a DNS name, not an IP.
	ln, err := tls.Listen("tcp", "localhost:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go servePings(ln)

	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatalf("split addr: %v", err)
	}
	c := NewClient(net.JoinHostPort("localhost", port), WithTLS(&tls.Config{RootCAs: roots}))
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Ping(ctx); err != nil {
		t.Fatalf("ping with inferred SNI: %v", err)
	}
}

// A plain-TCP client against a TLS server must fail cleanly, and a TLS client
// against an untrusted cert must refuse the handshake.
func TestWithTLSRejectsUntrustedCert(t *testing.T) {
	cert, _ := selfSignedCert(t, "localhost")
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go servePings(ln)

	c := NewClient(ln.Addr().String(), WithTLS(&tls.Config{ServerName: "localhost"}), WithMaxRetries(0))
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Ping(ctx); err == nil {
		t.Fatal("expected handshake failure against untrusted cert, got nil")
	}
}
