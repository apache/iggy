package binaryserialization

import (
	"encoding/binary"

	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
)

type TcpLogInRequest struct {
	iggcon.LogInRequest
}

func (request *TcpLogInRequest) Serialize() []byte {
	usernameBytes := []byte(request.Username)
	passwordBytes := []byte(request.Password)
	versionBytes := []byte(request.Version)
	contextBytes := []byte(request.Context)

	// Calculate total length
	totalLength := 2 + len(usernameBytes) + len(passwordBytes) +
		8 + len(versionBytes) + len(contextBytes)

	result := make([]byte, totalLength)
	position := 0

	// Username
	result[position] = byte(len(usernameBytes))
	position++
	copy(result[position:], usernameBytes)
	position += len(usernameBytes)

	// Password
	result[position] = byte(len(passwordBytes))
	position++
	copy(result[position:], passwordBytes)
	position += len(passwordBytes)

	// Version
	binary.LittleEndian.PutUint32(result[position:], uint32(len(versionBytes)))
	position += 4
	copy(result[position:], versionBytes)
	position += len(versionBytes)

	// Context
	binary.LittleEndian.PutUint32(result[position:], uint32(len(contextBytes)))
	position += 4
	copy(result[position:], contextBytes)

	return result
}
