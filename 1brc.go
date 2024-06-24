package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type ws struct {
	Min   int32
	Max   int32
	Mean  float64
	Count int32
}

func NewWs(temp int32) *ws {
	return &ws{
		Min:   temp,
		Max:   temp,
		Mean:  float64(temp),
		Count: 1,
	}
}

/*
*
!!gotta go faster!!

ideas

channels?
* split dataset into 1B / CPU chunks => [][]byte
* One map per channel, no overhead of locking/waiting


func findNextNewLine()
*/

func (w *ws) PutTemp(temp int32) *ws {
	min := w.Min
	max := w.Max
	if temp < w.Min {
		min = temp
	}
	if temp > w.Max {
		max = temp
	}
	mean := (w.Mean*float64(w.Count) + float64(temp)) / float64(w.Count+1)
	count := w.Count + 1

	return &ws{
		Min:   min,
		Max:   max,
		Mean:  mean,
		Count: count,
	}
}

func (w ws) String(name string) string {
	min := fmt.Sprintf("%.1f", float64(w.Min)/10.0)
	max := fmt.Sprintf("%.1f", float64(w.Max)/10.0)
	mean := fmt.Sprintf("%.1f", w.Mean/10.0)
	return fmt.Sprintf("%s=%s/%s/%s", name, min, mean, max)
}

func main() {
	start := time.Now()
	f, err := os.Open("measurements.txt")
	if err != nil {
		panic(err)
	}

	stations := make(map[string]*ws)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		split := strings.Split(line, ";")
		temp, err := strconv.Atoi(strings.Replace(split[1], ".", "", 1))
		if err != nil {
			panic(err)
		}

		name := split[0]
		val, ok := stations[name]
		if ok {
			stations[name] = val.PutTemp(int32(temp))
		} else {
			stations[name] = NewWs(int32(temp))
		}
	}
	f.Close()

	keys := make([]string, 0, len(stations))
	for key := range stations {
		keys = append(keys, key)
	}

	skeys := sort.StringSlice(keys)
	skeys.Sort()

	fmt.Printf("{%v", stations[skeys[0]].String(skeys[0]))
	for i, key := range skeys {
		if i == 0 {
			continue
		}
		fmt.Printf(", ")
		w := stations[key]
		fmt.Print(w.String(key))
	}
	fmt.Printf("}\n")

	fmt.Printf("Took %v s\n", time.Since(start))
}
