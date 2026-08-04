package main

import (
	"bufio"
	"fmt"
	"io"
	"kvStore/proto"
	"net"
	"os"
	"strings"
)

/*
- conn := dial tcp localhost 9000
- create a reader and writer bufio ( with conn in its input ) / so we can do the flush all in one syscall
- for loop that checks users input and calls send func
- send func : does a proto.encodecommand/flush or decodeResponse and print
*/

// client that connects to server
func main() {
	conn, err := net.Dial("tcp", "localhost:9000")
	if err != nil {
		fmt.Println("connect error:", err)
		os.Exit(1)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("store> ")
	for scanner.Scan() { // process input line by line
		line := scanner.Text() // get line that was just read ( SET a b)
		parts := strings.Fields(line)

		if len(parts) == 0 {
			fmt.Print("store> ")
			continue
		}

		switch parts[0] {
		case "set":
			if len(parts) != 3 {
				fmt.Println("usage: set <key> <value>")
				fmt.Print("store> ")
				continue
			}
			// []byte(parts[1]) -> 'h' 'e' 'l' 'l' 'o'
			send(reader, writer, proto.Command{Op: proto.OpSet, Key: []byte(parts[1]), Value: []byte(parts[2])})
		case "get":
			if len(parts) != 2 {
				fmt.Println("usage: get <key>")
				fmt.Print("store> ")
				continue
			}
			send(reader, writer, proto.Command{Op: proto.OpGet, Key: []byte(parts[1])})
		case "del":
			if len(parts) != 2 {
				fmt.Println("usage: del <key>")
				fmt.Print("store> ")
				continue
			}
			send(reader, writer, proto.Command{Op: proto.OpDel, Key: []byte(parts[1])})
		case "ping":
			send(reader, writer, proto.Command{Op: proto.OpPing})
		case "len":
			if len(parts) != 1 {
				fmt.Println("usage: len")
				fmt.Print("store> ")
				continue
			}
			send(reader, writer, proto.Command{Op: proto.OpLen})
		case "exit", "quit":
			return
		default:
			fmt.Println("unknown command")

		}
		fmt.Print("store> ")
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Scanner error: ", err)
	}
}

// encode, flush, then decode and display the response.
func send(reader io.Reader, writer *bufio.Writer, command proto.Command) {
	// first encode command to the bufio writer
	if err := proto.EncodeCommand(writer, command); err != nil {
		fmt.Println("encode/write error:", err)
		return
	}

	if err := writer.Flush(); err != nil {
		fmt.Println("flush error:", err)
		return
	}

	resp, err := proto.DecodeResponse(reader)
	if err != nil {
		if err == io.EOF {
			fmt.Println("server closed connection")
			os.Exit(1)
		}
		fmt.Println("decode error:", err)
		return
	}

	switch resp.Status {
	case proto.StatusOk:
		if len(resp.Value) > 0 {
			fmt.Println(string(resp.Value))
		} else {
			fmt.Println("OK")
		}
		// fmt.Println("Completed command")
	case proto.StatusNotFound:
		fmt.Println("key not found")
	case proto.StatusError:
		fmt.Println("server error")
	default:
		fmt.Println("unknown response status")
	}
}
