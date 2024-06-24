package main

import (
	"fmt"
	"strconv"
	"strings"
)

func main() {
	i, err := strconv.Atoi(strings.Replace("73.5", ".", "", 1))
	if err != nil {
		panic(err)
	}
	fmt.Println(i)
}
