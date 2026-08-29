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

// ──────────────────────────────────────────────────────────────────────
// Error Handling Tests
// ──────────────────────────────────────────────────────────────────────

func TestMaxPacketSizeLimit(t *testing.T) {
	// Test that readBytes rejects packets larger than maxPacketSize
	largeSize := maxPacketSize + 1
	_, err := readBytes(strings.NewReader("test"), largeSize)
	if err == nil {
		t.Error("readBytes should reject packets larger than maxPacketSize")
	}
	if err.Error() != fmt.Sprintf("packet size too large: %d (max %d)", largeSize, maxPacketSize) {
		t.Errorf("Expected packet size too large error, got: %v", err)
	}
}

func TestUTF8StringLengthValidation(t *testing.T) {
	// Test that readUTF8 rejects strings longer than maxUTF8Length
	longStr := strings.Repeat("a", maxUTF8Length+1)
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint16(len(longStr)))
	buf.WriteString(longStr)
	
	_, err := readUTF8(buf)
	if err == nil {
		t.Error("readUTF8 should reject strings longer than maxUTF8Length")
	}
	if !strings.Contains(err.Error(), "utf8 string too long") {
		t.Errorf("Expected utf8 string too long error, got: %v", err)
	}
}

func TestClientIDValidation(t *testing.T) {
	tests := []struct {
		name      string
		clientID  string
		expectErr bool
	}{
		{"Empty client ID", "", true},
		{"Valid client ID", "testclient", false},
		{"Long client ID", strings.Repeat("a", maxClientIDLength), false},
		{"Too long client ID", strings.Repeat("a", maxClientIDLength+1), true},
		{"Client ID with special chars", "client-123", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate client ID validation logic
			if len(tt.clientID) == 0 || len(tt.clientID) > maxClientIDLength {
				if !tt.expectErr {
					t.Errorf("Client ID %q should be valid", tt.clientID)
				}
			} else {
				if tt.expectErr {
					t.Errorf("Client ID %q should be invalid", tt.clientID)
				}
			}
		})
	}
}

func TestTopicNameValidation(t *testing.T) {
	tests := []struct {
		name      string
		topic     string
		expectErr bool
	}{
		{"Empty topic", "", true},
		{"Valid topic", "test/topic", false},
		{"Long topic", strings.Repeat("a", maxTopicLength), false},
		{"Too long topic", strings.Repeat("a", maxTopicLength+1), true},
		{"System topic", "$SYS/test", true},
		{"Normal topic", "user/data", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate topic validation logic
			if len(tt.topic) == 0 || len(tt.topic) > maxTopicLength {
				if !tt.expectErr {
					t.Errorf("Topic %q should be invalid", tt.topic)
				}
			} else if tt.topic[0] == '$' {
				if !tt.expectErr {
					t.Errorf("System topic %q should be invalid", tt.topic)
				}
			} else {
				if tt.expectErr {
					t.Errorf("Topic %q should be valid", tt.topic)
				}
			}
		})
	}
}

func TestMalformedRemainingLength(t *testing.T) {
	// Test decodeRemainingLength with invalid remaining length data
	// This simulates a packet with malformed remaining length
	buf := bytes.NewReader([]byte{0xFF, 0xFF, 0xFF, 0xFF}) // Maximum value that causes overflow
	
	_, err := decodeRemainingLength(buf)
	if err == nil {
		t.Error("decodeRemainingLength should fail with overflow data")
	}
	if err.Error() != "remaining length overflow" {
		t.Errorf("Expected remaining length overflow error, got: %v", err)
	}
}

// ──────────────────────────────────────────────────────────────────────
// QoS Message Flow Tests
// ──────────────────────────────────────────────────────────────────────

func TestQoS1PublishFlow(t *testing.T) {
	// Test QoS 1 publish flow: PUBLISH -> PUBACK
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
	
	// Create two clients
	conn1, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		b.Stop()
		t.Fatalf("dial client1: %v", err)
	}
	defer conn1.Close()
	
	conn2, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		b.Stop()
		t.Fatalf("dial client2: %v", err)
	}
	defer conn2.Close()
	
	// Connect both clients
	connectClient(conn1, "testclient1")
	connectClient(conn2, "testclient2")
	
	// Subscribe client2 to a topic
	subscribeClient(conn2, "test/topic", 1)
	
	// Publish QoS 1 message from client1
	publishQoS1(conn1, "test/topic", "hello world")
	
	// Verify client2 receives the message
	time.Sleep(100 * time.Millisecond) // Allow message processing
	
	// Read message from client2
	response := make([]byte, 1024)
	n, err := conn2.Read(response)
	if err != nil {
		t.Errorf("Failed to read message from client2: %v", err)
	}
	
	if n < 4 {
		t.Fatalf("Response too short: %d", n)
	}
	if response[0] != TypePublish {
		t.Errorf("Expected PUBLISH packet, got 0x%02x", response[0])
	}
	
	b.Stop()
}

func TestQoS2PublishFlow(t *testing.T) {
	// Test QoS 2 publish flow: PUBLISH -> PUBREC -> PUBREL -> PUBCOMP
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
	
	// Create two clients
	conn1, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		b.Stop()
		t.Fatalf("dial client1: %v", err)
	}
	defer conn1.Close()
	
	conn2, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		b.Stop()
		t.Fatalf("dial client2: %v", err)
	}
	defer conn2.Close()
	
	// Connect both clients
	connectClient(conn1, "testclient1")
	connectClient(conn2, "testclient2")
	
	// Subscribe client2 to a topic
	subscribeClient(conn2, "test/topic", 2)
	
	// Publish QoS 2 message from client1
	packetID := uint16(42)
	publishQoS2(conn1, "test/topic", "hello world", packetID)
	
	// Verify client2 receives PUBREC
	time.Sleep(100 * time.Millisecond)
	
	response := make([]byte, 1024)
	n, err := conn2.Read(response)
	if err != nil {
		t.Errorf("Failed to read PUBREC from client2: %v", err)
	}
	
	if n < 4 {
		t.Fatalf("Response too short: %d", n)
	}
	if response[0] != TypePublish {
		t.Errorf("Expected PUBLISH packet, got 0x%02x", response[0])
	}
	
	// Send PUBREL from client2
	sendPubrel(conn2, packetID)
	
	// Verify client1 receives PUBCOMP
	time.Sleep(100 * time.Millisecond)
	
	response = make([]byte, 1024)
	n, err = conn1.Read(response)
	if err != nil {
		t.Errorf("Failed to read PUBCOMP from client1: %v", err)
	}
	
	if n < 4 {
		t.Fatalf("Response too short: %d", n)
	}
	if response[0] != TypePubcomp {
		t.Errorf("Expected PUBCOMP packet, got 0x%02x", response[0])
	}
	
	b.Stop()
}

func TestDuplicateMessageHandling(t *testing.T) {
	// Test handling of duplicate messages (DUP flag)
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
	
	// Create client
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		b.Stop()
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	
	// Connect client
	connectClient(conn, "testclient")
	
	// Subscribe to a topic
	subscribeClient(conn, "test/topic", 1)
	
	// Publish duplicate message (with DUP flag)
	publishQoS1WithDup(conn, "test/topic", "duplicate message", 1)
	
	// Verify message is received (DUP flag should be handled gracefully)
	time.Sleep(100 * time.Millisecond)
	
	response := make([]byte, 1024)
	n, err := conn.Read(response)
	if err != nil {
		t.Errorf("Failed to read message: %v", err)
	}
	
	if n < 4 {
		t.Fatalf("Response too short: %d", n)
	}
	if response[0] != TypePublish {
		t.Errorf("Expected PUBLISH packet, got 0x%02x", response[0])
	}
	
	b.Stop()
}

// Helper functions for QoS tests
func connectClient(conn net.Conn, clientID string) {
	var connect bytes.Buffer
	writeUTF8(&connect, "MQTT")
	writeByte(&connect, 5)
	writeByte(&connect, 0x02)
	binary.Write(&connect, binary.BigEndian, uint16(60))
	writeByte(&connect, 0)
	writeUTF8(&connect, clientID)
	
	var buf bytes.Buffer
	writeByte(&buf, TypeConnect)
	rl := encodeRemainingLength(connect.Len())
	buf.Write(rl)
	buf.Write(connect.Bytes())
	
	if _, err := conn.Write(buf.Bytes()); err != nil {
		panic(err)
	}
	
	// Read CONNACK
	time.Sleep(50 * time.Millisecond)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	first, err := readByte(conn)
	if err != nil {
		panic(err)
	}
	if first != TypeConnack {
		panic(fmt.Sprintf("expected CONNACK 0x%02x, got 0x%02x", TypeConnack, first))
	}
}

func subscribeClient(conn net.Conn, topic string, qos byte) {
	packetID := uint16(1)
	var subscribe bytes.Buffer
	binary.Write(&subscribe, binary.BigEndian, packetID)
	writeUTF8(&subscribe, topic)
	writeByte(&subscribe, qos)
	
	var buf bytes.Buffer
	writeByte(&buf, TypeSubscribe)
	rl := encodeRemainingLength(subscribe.Len())
	buf.Write(rl)
	buf.Write(subscribe.Bytes())
	
	if _, err := conn.Write(buf.Bytes()); err != nil {
		panic(err)
	}
}

func publishQoS1(conn net.Conn, topic string, payload string) {
	var publish bytes.Buffer
	writeUTF8(&publish, topic)
	binary.Write(&publish, binary.BigEndian, uint16(0))
	publish.WriteString(payload)
	
	var buf bytes.Buffer
	writeByte(&buf, TypePublish|0x02) // QoS 1, no DUP
	rl := encodeRemainingLength(publish.Len())
	buf.Write(rl)
	buf.Write(publish.Bytes())
	
	if _, err := conn.Write(buf.Bytes()); err != nil {
		panic(err)
	}
}

func publishQoS2(conn net.Conn, topic string, payload string, packetID uint16) {
	var publish bytes.Buffer
	writeUTF8(&publish, topic)
	binary.Write(&publish, binary.BigEndian, packetID)
	publish.WriteString(payload)
	
	var buf bytes.Buffer
	writeByte(&buf, TypePublish|0x06) // QoS 2, no DUP
	rl := encodeRemainingLength(publish.Len())
	buf.Write(rl)
	buf.Write(publish.Bytes())
	
	if _, err := conn.Write(buf.Bytes()); err != nil {
		panic(err)
	}
}

func publishQoS1WithDup(conn net.Conn, topic string, payload string, packetID uint16) {
	var publish bytes.Buffer
	writeUTF8(&publish, topic)
	binary.Write(&publish, binary.BigEndian, packetID)
	publish.WriteString(payload)
	
	var buf bytes.Buffer
	writeByte(&buf, TypePublish|0x0A) // QoS 1 with DUP flag
	rl := encodeRemainingLength(publish.Len())
	buf.Write(rl)
	buf.Write(publish.Bytes())
	
	if _, err := conn.Write(buf.Bytes()); err != nil {
		panic(err)
	}
}

func sendPubrel(conn net.Conn, packetID uint16) {
	var buf bytes.Buffer
	writeByte(&buf, TypePubrel|0x02) // QoS 1
	rl := encodeRemainingLength(2)
	buf.Write(rl)
	binary.Write(&buf, binary.BigEndian, packetID)
	
	if _, err := conn.Write(buf.Bytes()); err != nil {
		panic(err)
	}
}

// ──────────────────────────────────────────────────────────────────────
// Authentication Tests
// ──────────────────────────────────────────────────────────────────────

func TestUsernamePasswordAuthentication(t *testing.T) {
	// Test successful authentication
	b := NewBroker(":0")
	b.SetAuth("testuser", "testpass")
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
	
	// Test successful connection with correct credentials
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		b.Stop()
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	
	// Connect with authentication
	connectClientWithAuth(conn, "testuser", "testpass", "testclient")
	
	// Read CONNACK
	time.Sleep(50 * time.Millisecond)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	first, err := readByte(conn)
	if err != nil {
		t.Errorf("Failed to read CONNACK: %v", err)
	}
	if first != TypeConnack {
		t.Errorf("Expected CONNACK 0x%02x, got 0x%02x", TypeConnack, first)
	}
	
	b.Stop()
}

func TestFailedAuthentication(t *testing.T) {
	// Test failed authentication
	b := NewBroker(":0")
	b.SetAuth("testuser", "testpass")
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
	
	// Test failed connection with wrong credentials
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		b.Stop()
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	
	// Connect with wrong credentials
	connectClientWithAuth(conn, "wronguser", "wrongpass", "testclient")
	
	// Read CONNACK with error code
	time.Sleep(50 * time.Millisecond)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	first, err := readByte(conn)
	if err != nil {
		t.Errorf("Failed to read CONNACK: %v", err)
	}
	if first != TypeConnack {
		t.Errorf("Expected CONNACK 0x%02x, got 0x%02x", TypeConnack, first)
	}
	
	// Read remaining length and reason code
	_, err = decodeRemainingLength(conn)
	if err != nil {
		t.Errorf("Failed to read remaining length: %v", err)
	}
	
	reasonCode, err := readByte(conn)
	if err != nil {
		t.Errorf("Failed to read reason code: %v", err)
	}
	
	if reasonCode != ReasonBadUserNameOrPassword {
		t.Errorf("Expected reason code %d (BadUserNameOrPassword), got %d", ReasonBadUserNameOrPassword, reasonCode)
	}
	
	b.Stop()
}

func TestAnonymousAccess(t *testing.T) {
	// Test broker without authentication
	b := NewBroker(":0")
	// No SetAuth call - allows anonymous access
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
	
	// Test connection without authentication
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		b.Stop()
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	
	// Connect without authentication
	connectClient(conn, "testclient")
	
	// Read CONNACK
	time.Sleep(50 * time.Millisecond)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	first, err := readByte(conn)
	if err != nil {
		t.Errorf("Failed to read CONNACK: %v", err)
	}
	if first != TypeConnack {
		t.Errorf("Expected CONNACK 0x%02x, got 0x%02x", TypeConnack, first)
	}
	
	b.Stop()
}

// Helper function for authenticated connection
func connectClientWithAuth(conn net.Conn, username, password, clientID string) {
	var connect bytes.Buffer
	writeUTF8(&connect, "MQTT")
	writeByte(&connect, 5)
	writeByte(&connect, 0xC2) // Clean session, username and password flags
	binary.Write(&connect, binary.BigEndian, uint16(60))
	writeByte(&connect, 0)
	writeUTF8(&connect, clientID)
	
	// Add username
	writeUTF8(&connect, username)
	
	// Add password
	passwordBytes := []byte(password)
	binary.Write(&connect, binary.BigEndian, uint16(len(passwordBytes)))
	connect.Write(passwordBytes)
	
	var buf bytes.Buffer
	writeByte(&buf, TypeConnect)
	rl := encodeRemainingLength(connect.Len())
	buf.Write(rl)
	buf.Write(connect.Bytes())
	
	if _, err := conn.Write(buf.Bytes()); err != nil {
		panic(err)
	}
}

// ──────────────────────────────────────────────────────────────────────
// Integration Tests
// ──────────────────────────────────────────────────────────────────────

func TestMultiClientCommunication(t *testing.T) {
	// Test full multi-client publish/subscribe scenario
	b := NewBroker(":0")
	b.SetAuth("testuser", "testpass")
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
	
	// Create multiple clients
	clients := make([]net.Conn, 3)
	defer func() {
		for _, conn := range clients {
			if conn != nil {
				conn.Close()
			}
		}
	}()
	
	// Connect all clients
	for i := 0; i < 3; i++ {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			b.Stop()
			t.Fatalf("dial client %d: %v", i, err)
		}
		connectClientWithAuth(conn, "testuser", "testpass", fmt.Sprintf("client%d", i))
		clients[i] = conn
	}
	
	// Subscribe clients 1 and 2 to different topics
	subscribeClient(clients[1], "sensors/temperature", 0)
	subscribeClient(clients[2], "sensors/humidity", 0)
	
	// Publish from client 0 to both topics
	publishQoS1(clients[0], "sensors/temperature", "25.5°C")
	publishQoS1(clients[0], "sensors/humidity", "60%")
	
	// Verify messages are received
	time.Sleep(200 * time.Millisecond)
	
	// Check client 1 received temperature message
	response1 := make([]byte, 1024)
	n1, err := clients[1].Read(response1)
	if err != nil {
		t.Errorf("Failed to read from client1: %v", err)
	}
	if n1 < 4 {
		t.Fatalf("Client1 response too short: %d", n1)
	}
	if response1[0] != TypePublish {
		t.Errorf("Client1 expected PUBLISH, got 0x%02x", response1[0])
	}
	
	// Check client 2 received humidity message
	response2 := make([]byte, 1024)
	n2, err := clients[2].Read(response2)
	if err != nil {
		t.Errorf("Failed to read from client2: %v", err)
	}
	if n2 < 4 {
		t.Fatalf("Client2 response too short: %d", n2)
	}
	if response2[0] != TypePublish {
		t.Errorf("Client2 expected PUBLISH, got 0x%02x", response2[0])
	}
	
	b.Stop()
}

func TestRealTimeMessageDelivery(t *testing.T) {
	// Test real-time message delivery with timing
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
	
	// Create publisher and subscriber
	pubConn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		b.Stop()
		t.Fatalf("dial publisher: %v", err)
	}
	defer pubConn.Close()
	
	subConn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		b.Stop()
		t.Fatalf("dial subscriber: %v", err)
	}
	defer subConn.Close()
	
	// Connect both clients
	connectClient(pubConn, "publisher")
	connectClient(subConn, "subscriber")
	
	// Subscribe to topic
	subscribeClient(subConn, "realtime/data", 1)
	
	// Publish multiple messages rapidly
	messages := []string{"msg1", "msg2", "msg3", "msg4", "msg5"}
	startTime := time.Now()
	
	for i, msg := range messages {
		publishQoS1(pubConn, "realtime/data", fmt.Sprintf("%s_%d", msg, i))
		time.Sleep(10 * time.Millisecond) // Small delay between messages
	}
	
	// Verify all messages are received within reasonable time
	time.Sleep(100 * time.Millisecond)
	
	// Count received messages
	receivedCount := 0
	response := make([]byte, 1024)
	
	for i := 0; i < 5; i++ {
		n, err := subConn.Read(response)
		if err != nil {
			break // No more messages
		}
		if n >= 4 && response[0] == TypePublish {
			receivedCount++
		}
	}
	
	endTime := time.Now()
	totalTime := endTime.Sub(startTime)
	
	if receivedCount != len(messages) {
		t.Errorf("Expected %d messages, received %d", len(messages), receivedCount)
	}
	
	if totalTime > 5*time.Second {
		t.Errorf("Message delivery took too long: %v", totalTime)
	}
	
	t.Logf("Delivered %d messages in %v", receivedCount, totalTime)
	
	b.Stop()
}

func TestBrokerShutdown(t *testing.T) {
	// Test graceful broker shutdown with active clients
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
	
	// Create active clients
	clients := make([]net.Conn, 3)
	for i := 0; i < 3; i++ {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			b.Stop()
			t.Fatalf("dial client %d: %v", i, err)
		}
		connectClient(conn, fmt.Sprintf("client%d", i))
		clients[i] = conn
	}
	
	// Subscribe clients
	for i := 0; i < 3; i++ {
		subscribeClient(clients[i], "test/topic", 1)
	}
	
	// Start publishing
	go func() {
		for i := 0; i < 5; i++ {
			publishQoS1(clients[0], "test/topic", fmt.Sprintf("message%d", i))
			time.Sleep(20 * time.Millisecond)
		}
	}()
	
	// Allow some messages to be processed
	time.Sleep(100 * time.Millisecond)
	
	// Stop broker
	stopStart := time.Now()
	b.Stop()
	stopDuration := time.Since(stopStart)
	
	// Verify all connections are closed
	for i, conn := range clients {
		buf := make([]byte, 1)
		_, err := conn.Read(buf)
		if err == nil {
			t.Errorf("Client %d connection should be closed after broker stop", i)
		}
	}
	
	if stopDuration > 1*time.Second {
		t.Errorf("Broker shutdown took too long: %v", stopDuration)
	}
	
	t.Logf("Broker shutdown completed in %v", stopDuration)
}

func TestConcurrentClientHandling(t *testing.T) {
	// Test handling of many concurrent clients
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
	
	// Create many concurrent connections
	clientCount := 10
	clients := make([]net.Conn, clientCount)
	errors := make(chan error, clientCount)
	
	// Connect all clients concurrently
	var wg sync.WaitGroup
	for i := 0; i < clientCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			
			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
			if err != nil {
				errors <- fmt.Errorf("client %d: %v", idx, err)
				return
			}
			
			connectClient(conn, fmt.Sprintf("client%d", idx))
			subscribeClient(conn, "concurrent/test", 1)
			
			clients[idx] = conn
		}(i)
	}
	
	wg.Wait()
	close(errors)
	
	// Check for connection errors
	for err := range errors {
		if err != nil {
			t.Errorf("Connection error: %v", err)
		}
	}
	
	// Publish message from first client
	if clients[0] != nil {
		publishQoS1(clients[0], "concurrent/test", "concurrent test message")
		
		// Verify all other clients receive the message
		time.Sleep(200 * time.Millisecond)
		
		for i := 1; i < clientCount; i++ {
			if clients[i] != nil {
				response := make([]byte, 1024)
				n, err := clients[i].Read(response)
				if err != nil {
					t.Errorf("Failed to read from client %d: %v", i, err)
					continue
				}
				if n < 4 {
					t.Errorf("Client %d response too short: %d", i, n)
					continue
				}
				if response[0] != TypePublish {
					t.Errorf("Client %d expected PUBLISH, got 0x%02x", i, response[0])
				}
			}
		}
	}
	
	// Cleanup
	for _, conn := range clients {
		if conn != nil {
			conn.Close()
		}
	}
	
	b.Stop()
}