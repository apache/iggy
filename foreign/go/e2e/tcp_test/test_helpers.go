package tcp_test

import (
	. "github.com/iggy-rs/iggy-go-client"
	. "github.com/iggy-rs/iggy-go-client/contracts"
	"math/rand"
	"strings"
	"time"
)

func createAuthorizedConnection() MessageStream {
	ms := createConnection()
	_, err := ms.LogIn(LogInRequest{
		Username: "iggy",
		Password: "iggy",
	})
	if err != nil {
		panic(err)
	}
	return ms
}

func createConnection() MessageStream {
	factory := &IggyClientFactory{}
	config := IggyConfiguration{
		BaseAddress: "127.0.0.1:8090",
		Protocol:    Tcp,
	}

	ms, err := factory.CreateMessageStream(config)
	if err != nil {
		panic(err)
	}
	return ms
}

func createRandomUInt32() uint32 {
	rand.New(rand.NewSource(time.Now().UnixNano()))
	return rand.Uint32()
}

func createRandomString(length int) string {
	// Define the character set from which to create the random string
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"

	// Initialize the random number generator with a seed based on the current time
	rand.New(rand.NewSource(time.Now().UnixNano()))

	// Create the random string
	result := make([]byte, length)
	for i := range result {
		result[i] = charset[rand.Intn(len(charset))]
	}
	return string(result)
}

func createRandomStringWithPrefix(prefix string, length int) string {
	// Define the character set from which to create the random string
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"

	// Initialize the random number generator with a seed based on the current time
	rand.New(rand.NewSource(time.Now().UnixNano()))

	// Create the random string
	result := make([]byte, length-len(prefix))
	for i := range result {
		result[i] = charset[rand.Intn(len(charset))]
	}
	return strings.ToLower(prefix) + string(result)
}
