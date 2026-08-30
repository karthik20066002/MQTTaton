package main

import (
	"encoding/binary"
	"fmt"
	"net"
)

// ──────────────────────────────────────────────────────────────────────
// Test Utility Functions
// ──────────────────────────────────────────────────────────────────────

// connectClient connects a client to the broker with the specified client ID
func connectClient(conn net.Conn, clientID string) {
	// CONNECT packet (MQTT 5.0 format)
	connectPacket := make([]byte, 0)
	
	// Fixed header
	connectPacket = append(connectPacket, TypeConnect) // CONNECT packet type
	connectPacket = append(connectPacket, 0x00) // Remaining length will be calculated later
	
	// Variable header
	connectPacket = append(connectPacket, 0x00, 0x04) // Protocol name length (4)
	connectPacket = append(connectPacket, 'M', 'Q', 'T', 'T') // Protocol name
	connectPacket = append(connectPacket, 0x05) // Protocol version (5)
	connectPacket = append(connectPacket, 0x02) // Connect flags (clean start=1, QoS=0, retain=0, password=0, username=0)
	connectPacket = append(connectPacket, 0x00, 0x3C) // Keep alive (60 seconds)
	
	// Properties (none for this simple client)
	connectPacket = append(connectPacket, 0x00) // Properties length (0)
	
	// Payload (client ID)
	clientIDBytes := []byte(clientID)
	connectPacket = append(connectPacket, byte(len(clientIDBytes)))
	connectPacket = append(connectPacket, clientIDBytes...)
	
	// Update remaining length
	remainingLength := len(connectPacket) - 2
	connectPacket[1] = byte(remainingLength)
	
	// Send CONNECT packet
	_, err := conn.Write(connectPacket)
	if err != nil {
		panic(fmt.Sprintf("Failed to send CONNECT packet: %v", err))
	}
	
	// Read CONNACK
	response := make([]byte, 4)
	_, err = conn.Read(response)
	if err != nil {
		panic(fmt.Sprintf("Failed to read CONNACK: %v", err))
	}
	
	// Verify CONNACK
	if response[0] != 0x20 || response[1] != 0x02 || response[2] != 0x00 || response[3] != 0x00 {
		panic(fmt.Sprintf("Invalid CONNACK: %x %x %x %x", response[0], response[1], response[2], response[3]))
	}
}

// connectClientWithAuth connects a client with username/password authentication
func connectClientWithAuth(conn net.Conn, username, password, clientID string) {
	// CONNECT packet with username and password flags set
	connectPacket := make([]byte, 0)
	
	// Fixed header
	connectPacket = append(connectPacket, TypeConnect) // CONNECT packet type
	connectPacket = append(connectPacket, 0x00) // Remaining length will be calculated later
	
	// Variable header
	connectPacket = append(connectPacket, 0x00, 0x04) // Protocol name length (4)
	connectPacket = append(connectPacket, 'M', 'Q', 'T', 'T') // Protocol name
	connectPacket = append(connectPacket, 0x05) // Protocol version (5)
	
	// Connect flags (clean start=1, QoS=0, retain=0, password=1, username=1)
	connectPacket = append(connectPacket, 0xC2) 
	
	connectPacket = append(connectPacket, 0x00, 0x3C) // Keep alive (60 seconds)
	
	// Properties (none for this simple client)
	connectPacket = append(connectPacket, 0x00) // Properties length (0)
	
	// Payload (client ID)
	clientIDBytes := []byte(clientID)
	connectPacket = append(connectPacket, byte(len(clientIDBytes)))
	connectPacket = append(connectPacket, clientIDBytes...)
	
	// Username
	usernameBytes := []byte(username)
	connectPacket = append(connectPacket, byte(len(usernameBytes)))
	connectPacket = append(connectPacket, usernameBytes...)
	
	// Password
	passwordBytes := []byte(password)
	connectPacket = append(connectPacket, byte(len(passwordBytes)))
	connectPacket = append(connectPacket, passwordBytes...)
	
	// Update remaining length
	remainingLength := len(connectPacket) - 2
	connectPacket[1] = byte(remainingLength)
	
	// Send CONNECT packet
	_, err := conn.Write(connectPacket)
	if err != nil {
		panic(fmt.Sprintf("Failed to send CONNECT packet: %v", err))
	}
	
	// Read CONNACK
	response := make([]byte, 4)
	_, err = conn.Read(response)
	if err != nil {
		panic(fmt.Sprintf("Failed to read CONNACK: %v", err))
	}
	
	// Verify CONNACK
	if response[0] != 0x20 || response[1] != 0x02 || response[2] != 0x00 || response[3] != 0x00 {
		panic(fmt.Sprintf("Invalid CONNACK: %x %x %x %x", response[0], response[1], response[2], response[3]))
	}
}

// subscribeClient subscribes to a topic with specified QoS
func subscribeClient(conn net.Conn, topic string, qos byte) {
	// SUBSCRIBE packet
	subscribePacket := make([]byte, 0)
	
	// Fixed header
	subscribePacket = append(subscribePacket, TypeSubscribe|0x02) // SUBSCRIBE packet type with QoS 1
	subscribePacket = append(subscribePacket, 0x00) // Remaining length will be calculated later
	
	// Variable header
	subscribePacket = append(subscribePacket, 0x00, 0x0A) // Packet ID (10)
	
	// Payload (topic and QoS)
	topicBytes := []byte(topic)
	subscribePacket = append(subscribePacket, byte(len(topicBytes)))
	subscribePacket = append(subscribePacket, topicBytes...)
	subscribePacket = append(subscribePacket, qos) // QoS level
	
	// Update remaining length
	remainingLength := len(subscribePacket) - 2
	subscribePacket[1] = byte(remainingLength)
	
	// Send SUBSCRIBE packet
	_, err := conn.Write(subscribePacket)
	if err != nil {
		panic(fmt.Sprintf("Failed to send SUBSCRIBE packet: %v", err))
	}
	
	// Read SUBACK
	response := make([]byte, 5)
	_, err = conn.Read(response)
	if err != nil {
		panic(fmt.Sprintf("Failed to read SUBACK: %v", err))
	}
	
	// Verify SUBACK
	if response[0] != 0x90 || response[1] != 0x03 || response[2] != 0x00 || response[3] != 0x0A || response[4] != qos {
		panic(fmt.Sprintf("Invalid SUBACK: %x %x %x %x %x", response[0], response[1], response[2], response[3], response[4]))
	}
}

// publishQoS1 publishes a message with QoS 1
func publishQoS1(conn net.Conn, topic string, payload string) {
	// PUBLISH packet with QoS 1
	publishPacket := make([]byte, 0)
	
	// Fixed header
	publishPacket = append(publishPacket, TypePublish|0x02) // PUBLISH packet type with QoS 1
	publishPacket = append(publishPacket, 0x00) // Remaining length will be calculated later
	
	// Variable header
	topicBytes := []byte(topic)
	publishPacket = append(publishPacket, byte(len(topicBytes)))
	publishPacket = append(publishPacket, topicBytes...)
	
	// Packet ID
	publishPacket = append(publishPacket, 0x00, 0x0B) // Packet ID (11)
	
	// Payload
	payloadBytes := []byte(payload)
	publishPacket = append(publishPacket, payloadBytes...)
	
	// Update remaining length
	remainingLength := len(publishPacket) - 2
	publishPacket[1] = byte(remainingLength)
	
	// Send PUBLISH packet
	_, err := conn.Write(publishPacket)
	if err != nil {
		panic(fmt.Sprintf("Failed to send PUBLISH packet: %v", err))
	}
}

// publishQoS2 publishes a message with QoS 2
func publishQoS2(conn net.Conn, topic string, payload string, packetID uint16) {
	// PUBLISH packet with QoS 2
	publishPacket := make([]byte, 0)
	
	// Fixed header
	publishPacket = append(publishPacket, TypePublish|0x06) // PUBLISH packet type with QoS 2
	publishPacket = append(publishPacket, 0x00) // Remaining length will be calculated later
	
	// Variable header
	topicBytes := []byte(topic)
	publishPacket = append(publishPacket, byte(len(topicBytes)))
	publishPacket = append(publishPacket, topicBytes...)
	
	// Packet ID
	packetIDBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(packetIDBytes, packetID)
	publishPacket = append(publishPacket, packetIDBytes...)
	
	// Payload
	payloadBytes := []byte(payload)
	publishPacket = append(publishPacket, payloadBytes...)
	
	// Update remaining length
	remainingLength := len(publishPacket) - 2
	publishPacket[1] = byte(remainingLength)
	
	// Send PUBLISH packet
	_, err := conn.Write(publishPacket)
	if err != nil {
		panic(fmt.Sprintf("Failed to send PUBLISH packet: %v", err))
	}
}

// sendPubrel sends a PUBREL packet for QoS 2 message completion
func sendPubrel(conn net.Conn, packetID uint16) {
	// PUBREL packet
	pubrelPacket := make([]byte, 0)
	
	// Fixed header
	pubrelPacket = append(pubrelPacket, TypePubrel|0x02) // PUBREL packet type with QoS 1
	pubrelPacket = append(pubrelPacket, 0x02) // Remaining length (2)
	
	// Variable header (packet ID)
	packetIDBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(packetIDBytes, packetID)
	pubrelPacket = append(pubrelPacket, packetIDBytes...)
	
	// Send PUBREL packet
	_, err := conn.Write(pubrelPacket)
	if err != nil {
		panic(fmt.Sprintf("Failed to send PUBREL packet: %v", err))
	}
}