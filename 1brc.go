package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"
)

type ws struct {
	Min   int32
	Max   int32
	Sum   int32
	Count int32
}

func NewWs(temp int32) *ws {
	return &ws{
		Min:   temp,
		Max:   temp,
		Sum:   temp,
		Count: 1,
	}
}

/*
*
!!gotta go faster!!

ideas

* split dataset into 1B / CPU chunks => [][]byte


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
	sum := w.Sum + temp
	count := w.Count + 1

	return &ws{
		Min:   min,
		Max:   max,
		Sum:   sum,
		Count: count,
	}
}

func merge(w *ws, x *ws) *ws {
	var min int32
	var max int32
	if w.Min < x.Min {
		min = w.Min
	} else {
		min = x.Min
	}
	if w.Max > x.Max {
		max = w.Max
	} else {
		max = x.Max
	}

	count := w.Count + x.Count
	sum := w.Sum + x.Sum

	return &ws{
		Min:   min,
		Max:   max,
		Sum:   sum,
		Count: count,
	}
}

func (w ws) String(name string) string {
	min := fmt.Sprintf("%.1f", float64(w.Min)/10.0)
	max := fmt.Sprintf("%.1f", float64(w.Max)/10.0)
	mean := fmt.Sprintf("%.1f", float64(w.Sum)/float64(w.Count)/10.0)
	return fmt.Sprintf("%s=%s/%s/%s", name, min, mean, max)
}

func main() {
	start := time.Now()
	f, err := os.Open("measurements.txt")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	fstat, err := f.Stat()
	if err != nil {
		panic(err)
	}

	fsize := fstat.Size()
	segments := runtime.NumCPU()
	ssize := fsize / int64(segments)

	w := make([]map[string]*ws, segments)
	offset := int64(0)
	var wg sync.WaitGroup
	for i := 0; i < segments; i++ {
		nextOffset, segmentSize := getOffsetAndSize(offset, ssize, fsize, f)
		wg.Add(1)
		go func(f *os.File, offset int64, size int64, i int) {
			defer wg.Done()
			w[i] = process(f, offset, size, i)
		}(f, offset, segmentSize, i)
		offset = nextOffset
	}

	wg.Wait()
	combinedResults := make(map[string]*ws)
	for _, segmentMap := range w {
		for key, val := range segmentMap {
			if existing, found := combinedResults[key]; found {
				combinedResults[key] = merge(existing, val)
			} else {
				combinedResults[key] = val
			}
		}
	}

	sortAndPrint(&combinedResults)
	fmt.Printf("Took %v s\n", time.Since(start))
}

func getOffsetAndSize(startOffset int64, targetSize int64, fsize int64, f *os.File) (int64, int64) {
	nextOffset := startOffset + targetSize
	if nextOffset >= fsize {
		return fsize, targetSize
	}

	size := targetSize
	b := make([]byte, 1)
	for {
		s, err := f.ReadAt(b, nextOffset)
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		if s < len(b) {
			panic("sheit")
		}

		if b[0] == '\n' {
			size += 1
			break
		}

		size += 1
		nextOffset++
	}

	return nextOffset + 1, int64(size)
}

func process(f *os.File, offset int64, size int64, segment int) map[string]*ws {
	w := make(map[string]*ws)

	secR := io.NewSectionReader(f, offset, size)
	r := bufio.NewReader(secR)

	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		processLine(line[:len(line)], &w)
	}

	return w
}

func processLine(line []byte, stations *map[string]*ws) {
	i := bytes.IndexByte(line, ';')
	end := bytes.IndexByte(line, '\n')

	name := string(line[:i])
	start := i + 1

	tempstr := line[start:end]
	temp, err := bytesToInt16(tempstr)
	if err != nil {
		panic(err)
	}

	val, ok := (*stations)[name]
	if ok {
		(*stations)[name] = val.PutTemp(int32(temp))
	} else {
		(*stations)[name] = NewWs(int32(temp))
	}
}

func bytesToInt16(b []byte) (int16, error) {
	var result int16
	var negative bool

	if len(b) > 0 && b[0] == '-' {
		negative = true
		b = b[1:]
	}

	for _, digit := range b {
		if digit == '.' {
			continue
		}
		if digit >= '0' && digit <= '9' {
			result = result*10 + int16(digit-'0')
		} else {
			return 0, fmt.Errorf("invalid byte sequence: %s", string(b))
		}
	}

	if negative {
		result = -result
	}

	return result, nil
}

func sortAndPrint(stations *map[string]*ws) {
	keys := make([]string, 0, len(*stations))
	for key := range *stations {
		keys = append(keys, key)
	}

	skeys := sort.StringSlice(keys)
	skeys.Sort()

	fmt.Printf("{%v", (*stations)[skeys[0]].String(skeys[0]))
	for i, key := range skeys {
		if i == 0 {
			continue
		}
		fmt.Printf(", ")
		w := (*stations)[key]
		fmt.Print(w.String(key))
	}
	fmt.Printf("}\n")
}
