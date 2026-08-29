package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

func TestTopicMatch(t *testing.T) {
	tests := []struct {
		filter string
		topic  string
		want   bool
	}{
		{"foo/bar", "foo/bar", true},
		{"foo/bar", "foo/baz", false},
		{"foo/+", "foo/bar", true},
		{"foo/+", "foo/bar/baz", false},
		{"foo/#", "foo/bar/baz", true},
		{"foo/#", "foo/bar", true},
		{"foo/#", "bar/baz", false},
		{"#", "anything/goes", true},
		{"+", "single", true},
		{"+", "two/levels", false},
		{"sport/tennis/player1/#", "sport/tennis/player1/ranking", true},
		{"sport/tennis/player1/+", "sport/tennis/player1/ranking", true},
		{"sport/tennis/player1/+", "sport/tennis/player1", false},
		{"sport/tennis/#", "sport/tennis", true},
		{"sport/+/player1/#", "sport/tennis/player1/ranking", true},
	}
	for _, tt := range tests {
		got := topicMatch(tt.filter, tt.topic)
		if got != tt.want {
			t.Errorf("topicMatch(%q, %q) = %v, want %v", tt.filter, tt.topic, got, tt.want)
		}
	}
}

func TestRemainingLength(t *testing.T) {
	tests := []struct {
		n    int
		want []byte
	}{
		{0, []byte{0x00}},
		{1, []byte{0x01}},
		{127, []byte{0x7f}},
		{128, []byte{0x80, 0x01}},
		{16383, []byte{0xff, 0x7f}},
		{16384, []byte{0x80, 0x80, 0x01}},
		{268435455, []byte{0xff, 0xff, 0xff, 0x7f}},
	}
	for _, tt := range tests {
		enc := encodeRemainingLength(tt.n)
		if !bytes.Equal(enc, tt.want) {
			t.Errorf("encodeRemainingLength(%d) = %v, want %v", tt.n, enc, tt.want)
		}
		got, err := decodeRemainingLength(bytes.NewReader(enc))
		if err != nil {
			t.Errorf("decodeRemainingLength(%v) error: %v", enc, err)
			continue
		}
		if got != tt.n {
			t.Errorf("decodeRemainingLength(%v) = %d, want %d", enc, got, tt.n)
		}
	}
}

func TestEncodePublish(t *testing.T) {
	pkt := makePublishPkt("sport/tennis", 1, false, []byte("score"))
	if len(pkt) < 4 {
		t.Fatalf("publish packet too short: %d", len(pkt))
	}
	if pkt[0] != 0x32 {
		t.Errorf("first byte = 0x%02x, want 0x32", pkt[0])
	}
	if !bytes.Contains(pkt, []byte("sport/tennis")) {
		t.Error("topic not found in publish packet")
	}
}

func TestBrokerAcceptsConnections(t *testing.T) {
	b := NewBroker(":0")
	go b.listen()
	var addr string
	for i := 0; i < 100; i++ {
		b.mu.RLock()
		ln := b.listener
		b.mu.RUnlock()
		if ln != nil {
			addr = ln.Addr().String()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if addr == "" {
		b.Stop()
		t.Fatal("listener not ready")
	}
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		b.Stop()
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	b.Stop()
	_ = conn
}

func TestClientReceiveConnAck(t *testing.T) {
	b := NewBroker(":0")
	go b.listen()
	var addr string
	for i := 0; i < 100; i++ {
		b.mu.RLock()
		ln := b.listener
		b.mu.RUnlock()
		if ln != nil {
			addr = ln.Addr().String()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if addr == "" {
		b.Stop()
		t.Fatal("listener not ready")
	}
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		b.Stop()
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	defer b.Stop()

	var connect bytes.Buffer
	writeUTF8(&connect, "MQTT")
	writeByte(&connect, 5)
	writeByte(&connect, 0x02)
	binary.Write(&connect, binary.BigEndian, uint16(60))
	writeByte(&connect, 0)
	writeUTF8(&connect, "testclient")

	var buf bytes.Buffer
	writeByte(&buf, TypeConnect)
	rl := encodeRemainingLength(connect.Len())
	buf.Write(rl)
	buf.Write(connect.Bytes())

	if _, err := conn.Write(buf.Bytes()); err != nil {
		t.Fatalf("write connect: %v", err)
	}

	// Give broker time to process and respond
	time.Sleep(50 * time.Millisecond)

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	first, err := readByte(conn)
	if err != nil {
		t.Fatalf("read CONNACK: %v", err)
	}
	if first != TypeConnack {
		t.Fatalf("expected CONNACK 0x%02x, got 0x%02x", TypeConnack, first)
	}
	rl2, err := decodeRemainingLength(conn)
	if err != nil {
		t.Fatalf("read remaining length: %v", err)
	}
	ackData := make([]byte, rl2)
	if _, err := io.ReadFull(conn, ackData); err != nil {
		t.Fatalf("read ack data: %v", err)
	}
	if len(ackData) < 2 {
		t.Fatalf("connack too short: %d", len(ackData))
	}
	if ackData[1] != ReasonSuccess {
		t.Errorf("connack reason code = %d, want %d", ackData[1], ReasonSuccess)
	}
}

func TestMakeConnAck(t *testing.T) {
	pkt := makeConnAck(true, ReasonSuccess)
	if pkt[0] != TypeConnack {
		t.Errorf("first byte = 0x%02x", pkt[0])
	}
	if pkt[1] != 0x02 {
		t.Errorf("remaining length = %d, want 2", pkt[1])
	}
	if pkt[2] != 0x01 {
		t.Errorf("session present = %d, want 1", pkt[2])
	}
	if pkt[3] != ReasonSuccess {
		t.Errorf("reason code = %d, want %d", pkt[3], ReasonSuccess)
	}
}

func TestMakeSubAck(t *testing.T) {
	pkt := makeSubAck(42, []byte{ReasonSuccess, ReasonSuccess})
	if pkt[0] != TypeSuback {
		t.Errorf("first byte = 0x%02x", pkt[0])
	}
	var pid uint16
	binary.Read(bytes.NewReader(pkt[2:4]), binary.BigEndian, &pid)
	if pid != 42 {
		t.Errorf("packet id = %d, want 42", pid)
	}
	if len(pkt) != 6 {
		t.Errorf("length = %d, want 6", len(pkt))
	}
}

func TestMakePublishPktQoS0(t *testing.T) {
	pkt := makePublishPkt("test/topic", 0, true, []byte("hello"))
	if pkt[0] != 0x31 {
		t.Errorf("first byte = 0x%02x, want 0x31", pkt[0])
	}
}

func TestMakePublishPktQoS1(t *testing.T) {
	pkt := makePublishPkt("test/topic", 1, false, []byte("hello"))
	if pkt[0] != 0x32 {
		t.Errorf("first byte = 0x%02x, want 0x32", pkt[0])
	}
}

func TestMakePublishPktQoS2(t *testing.T) {
	pkt := makePublishPkt("test/topic", 2, false, []byte("hello"))
	if pkt[0] != 0x34 {
		t.Errorf("first byte = 0x%02x, want 0x34", pkt[0])
	}
}

func TestMakePublishPktContainsPayload(t *testing.T) {
	payload := []byte("hello world")
	pkt := makePublishPkt("a/b", 0, false, payload)
	if !bytes.Contains(pkt, payload) {
		t.Error("payload not found in publish packet")
	}
}

func TestEncodeUTF8(t *testing.T) {
	var buf bytes.Buffer
	if err := writeUTF8(&buf, "hello"); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	if len(data) != 7 {
		t.Fatalf("expected 7 bytes, got %d", len(data))
	}
	var n uint16
	binary.Read(bytes.NewReader(data[:2]), binary.BigEndian, &n)
	if n != 5 {
		t.Errorf("length prefix = %d, want 5", n)
	}
}

func TestDecodeUTF8(t *testing.T) {
	var buf bytes.Buffer
	writeUTF8(&buf, "testtopic")
	r := bytes.NewReader(buf.Bytes())
	s, err := readUTF8(r)
	if err != nil {
		t.Fatal(err)
	}
	if s != "testtopic" {
		t.Errorf("got %q, want %q", s, "testtopic")
	}
}

func TestMakePubackPkt(t *testing.T) {
	pkt := makePubackPkt(123)
	if pkt[0] != TypePuback {
		t.Errorf("first byte = 0x%02x", pkt[0])
	}
	var pid uint16
	binary.Read(bytes.NewReader(pkt[2:4]), binary.BigEndian, &pid)
	if pid != 123 {
		t.Errorf("packet id = %d, want 123", pid)
	}
}

func TestMakePingrespPkt(t *testing.T) {
	pkt := makePingrespPkt()
	if len(pkt) != 2 {
		t.Fatalf("length = %d, want 2", len(pkt))
	}
	if pkt[0] != TypePingresp {
		t.Errorf("first byte = 0x%02x", pkt[0])
	}
	if pkt[1] != 0x00 {
		t.Errorf("second byte = 0x%02x", pkt[1])
	}
}

func TestMakeUnsubAck(t *testing.T) {
	pkt := makeUnsubAck(5, []byte{ReasonSuccess})
	if pkt[0] != TypeUnsuback {
		t.Errorf("first byte = 0x%02x", pkt[0])
	}
	var pid uint16
	binary.Read(bytes.NewReader(pkt[2:4]), binary.BigEndian, &pid)
	if pid != 5 {
		t.Errorf("packet id = %d, want 5", pid)
	}
}

func TestMakePubrecPkt(t *testing.T) {
	pkt := makePubrecPkt(10)
	if pkt[0] != TypePubrec {
		t.Errorf("first byte = 0x%02x", pkt[0])
	}
	var pid uint16
	binary.Read(bytes.NewReader(pkt[2:4]), binary.BigEndian, &pid)
	if pid != 10 {
		t.Errorf("packet id = %d, want 10", pid)
	}
}

func TestMakePubrelPkt(t *testing.T) {
	pkt := makePubrelPkt(7)
	if pkt[0] != TypePubrel {
		t.Errorf("first byte = 0x%02x", pkt[0])
	}
	var pid uint16
	binary.Read(bytes.NewReader(pkt[2:4]), binary.BigEndian, &pid)
	if pid != 7 {
		t.Errorf("packet id = %d, want 7", pid)
	}
}

func TestMakePubcompPkt(t *testing.T) {
	pkt := makePubcompPkt(3)
	if pkt[0] != TypePubcomp {
		t.Errorf("first byte = 0x%02x", pkt[0])
	}
	var pid uint16
	binary.Read(bytes.NewReader(pkt[2:4]), binary.BigEndian, &pid)
	if pid != 3 {
		t.Errorf("packet id = %d, want 3", pid)
	}
}

func TestTopicMatchEdgeCases(t *testing.T) {
	// Empty strings
	if !topicMatch("", "") {
		t.Error("empty filter should match empty topic")
	}
	// Single level
	if !topicMatch("+", "a") {
		t.Error("+ should match single level")
	}
	if topicMatch("+", "a/b") {
		t.Error("+ should not match multiple levels")
	}
	// Multi level
	if !topicMatch("#", "") {
		t.Error("# should match empty topic")
	}
	if !topicMatch("#", "a/b/c") {
		t.Error("# should match any topic")
	}
}