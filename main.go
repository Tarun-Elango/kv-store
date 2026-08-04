package main

import (
	"bufio"
	"fmt"
	"kvStore/store"
	"os"
	"strings"
)

func main() {
	st := store.NewStore[string, string]() // pointer to store
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Print("store> ")
	for scanner.Scan() {

		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		switch parts[0] {
		case "len":
			fmt.Println(st.Len())
		case "set":
			if len(parts) != 3 {
				fmt.Println("usage: set <key> <value>")
				continue
			}

			st.Set(parts[1], parts[2])
			fmt.Println("OK")
		case "get":
			if len(parts) != 2 {
				fmt.Println("usage: get <key> <value>")
				continue
			}
			v, ok := st.Get(parts[1])
			if !ok {
				fmt.Print("Key not found")
			}
			fmt.Println(v)

		case "del":
			if len(parts) != 2 {
				fmt.Println("usage: del <key>")
				continue
			}
			st.Delete(parts[1])
			fmt.Println("OK")
		case "exit", "quit":
			return
		default:
			fmt.Println("unknown command")
			fmt.Println("store> ")
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error:", err)
	}
}
