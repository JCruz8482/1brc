package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
	f, err := os.Open("measurements.txt")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	wf, err := os.Create("measurements-small.txt")
	if err != nil {
		panic(err)
	}
	defer wf.Close()

	i := 0
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		val := scanner.Text()
		wf.WriteString(fmt.Sprintf("%v\n", val))
		i++
		if i > 3000000 {
			break
		}
	}
}
