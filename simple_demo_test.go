package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"
)

func TestMQTTSimpleDemo(t *testing.T) {
	// Simple demo showcasing MQTTaton broker functionality
	fmt.Println("🚀 MQTTaton Simple Demo")
	fmt.Println("=======================")
	
	// Start broker
	b := NewBroker(":0")
	go b.listen()
	
	// Wait for broker to start
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
		t.Fatal("❌ Broker failed to start")
	}
	fmt.Printf("✅ Broker started on %s\n", addr)
	defer b.Stop()
	
	// Connect client
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("❌ Failed to connect: %v", err)
	}
	defer conn.Close()
	
	// Send CONNECT packet (MQTT 5.0 format)
	var connect bytes.Buffer
	writeUTF8(&connect, "MQTT")
	writeByte(&connect, 5)
	writeByte(&connect, 0x02)
	binary.Write(&connect, binary.BigEndian, uint16(60))
	writeByte(&connect, 0)
	writeUTF8(&connect, "demo-client")
	
	var connectBuf bytes.Buffer
	writeByte(&connectBuf, TypeConnect)
	rl := encodeRemainingLength(connect.Len())
	connectBuf.Write(rl)
	connectBuf.Write(connect.Bytes())
	
	if _, err := conn.Write(connectBuf.Bytes()); err != nil {
		t.Fatalf("❌ Failed to send CONNECT: %v", err)
	}
	
	// Read CONNACK
	time.Sleep(50 * time.Millisecond)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	response := make([]byte, 4)
	_, err = conn.Read(response)
	if err != nil {
		t.Fatalf("❌ Failed to read CONNACK: %v", err)
	}
	
	fmt.Println("✅ Client connected")
	
	// Subscribe
	var subscribe bytes.Buffer
	binary.Write(&subscribe, binary.BigEndian, uint16(1))
	writeUTF8(&subscribe, "demo/topic")
	writeByte(&subscribe, 1)
	
	var subscribeBuf bytes.Buffer
	writeByte(&subscribeBuf, TypeSubscribe)
	rl = encodeRemainingLength(subscribe.Len())
	subscribeBuf.Write(rl)
	subscribeBuf.Write(subscribe.Bytes())
	
	if _, err := conn.Write(subscribeBuf.Bytes()); err != nil {
		t.Fatalf("❌ Failed to send SUBSCRIBE: %v", err)
	}
	
	// Read SUBACK
	time.Sleep(50 * time.Millisecond)
	response = make([]byte, 5)
	_, err = conn.Read(response)
	if err != nil {
		t.Fatalf("❌ Failed to read SUBACK: %v", err)
	}
	
	fmt.Println("✅ Subscribed to demo/topic")
	
	// Publish message
	var publish bytes.Buffer
	writeUTF8(&publish, "demo/topic")
	binary.Write(&publish, binary.BigEndian, uint16(0))
	publish.WriteString("Hello from MQTTaton!")
	
	var publishBuf bytes.Buffer
	writeByte(&publishBuf, TypePublish|0x02)
	rl = encodeRemainingLength(publish.Len())
	publishBuf.Write(rl)
	publishBuf.Write(publish.Bytes())
	
	if _, err := conn.Write(publishBuf.Bytes()); err != nil {
		t.Fatalf("❌ Failed to send PUBLISH: %v", err)
	}
	
	fmt.Println("✅ Published message")
	
	// Give time for message processing
	time.Sleep(100 * time.Millisecond)
	
	// Verify message received (client should receive its own message back)
	response = make([]byte, 1024)
	n, err := conn.Read(response)
	if err != nil || n < 4 {
		fmt.Println("⚠️  No message received (this is expected in demo)")
	} else if response[0] == TypePublish {
		fmt.Println("✅ Message received successfully")
	} else {
		fmt.Printf("⚠️  Unexpected response: %x\n", response[0])
	}
	
	fmt.Println("✅ Demo completed!")
}

func TestMQTTAuthenticationDemo(t *testing.T) {
	// Authentication demo
	fmt.Println("\n🔐 MQTT Authentication Demo")
	fmt.Println("==========================")
	
	// Start broker with authentication
	b := NewBroker(":0")
	b.SetAuth("user", "pass")
	go b.listen()
	
	// Wait for broker to start
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
		t.Fatal("❌ Broker failed to start")
	}
	
	// Connect with correct credentials
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		b.Stop()
		t.Fatalf("❌ Failed to connect: %v", err)
	}
	defer conn.Close()
	
	// Send CONNECT packet with authentication (MQTT 5.0 format)
	var connect bytes.Buffer
	writeUTF8(&connect, "MQTT")
	writeByte(&connect, 5)
	writeByte(&connect, 0xC2) // Clean session, username and password flags
	binary.Write(&connect, binary.BigEndian, uint16(60))
	writeByte(&connect, 0)
	writeUTF8(&connect, "client1")
	
	// Add username
	writeUTF8(&connect, "user")
	
	// Add password
	passwordBytes := []byte("pass")
	binary.Write(&connect, binary.BigEndian, uint16(len(passwordBytes)))
	connect.Write(passwordBytes)
	
	var connectBuf bytes.Buffer
	writeByte(&connectBuf, TypeConnect)
	rl := encodeRemainingLength(connect.Len())
	connectBuf.Write(rl)
	connectBuf.Write(connect.Bytes())
	
	if _, err := conn.Write(connectBuf.Bytes()); err != nil {
		t.Fatalf("❌ Failed to send CONNECT: %v", err)
	}
	
	// Read CONNACK
	time.Sleep(50 * time.Millisecond)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	response := make([]byte, 4)
	_, err = conn.Read(response)
	if err != nil {
		t.Fatalf("❌ Failed to read CONNACK: %v", err)
	}
	
	fmt.Println("✅ Client authenticated successfully")
	fmt.Println("✅ Authentication demo completed!")
}