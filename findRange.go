package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func main() {
	f, err := os.Open("measurements-med.txt")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	min := float64(0)
	max := float64(0)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		split := strings.Split(line, ";")

		temp, err := strconv.ParseFloat(split[1], 64)
		if err != nil {
			panic(err)
		}

		if temp < min {
			min = temp
		}
		if temp > max {
			max = temp
		}
	}

	fmt.Printf("min = %v\n", min)
	fmt.Printf("max = %v\n", max)
}
