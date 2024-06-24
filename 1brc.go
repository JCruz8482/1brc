package main

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
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
	mean := (w.Mean*float64(w.Count) + float64(temp)) / float64(w.Count+1)
	count := w.Count + 1

	return &ws{
		Min:   min,
		Max:   max,
		Mean:  mean,
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
	mean := (w.Mean*float64(w.Count) + x.Mean*float64(x.Count)) / float64(count)

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
	defer f.Close()

	fstat, err := f.Stat()
	if err != nil {
		panic(err)
	}

	fsize := fstat.Size()
	segments := runtime.NumCPU()
	ssize := fsize / int64(segments)
	// for each segment
	// create a new go routine to read file from bytes offset for ssize + offset to newline
	// to determine offset and end, must find next newline char
	// An array of []bytes of size ssize + new offset per go routine can be fed to f.ReadAt()
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
	/**
	combinedResults := make(map[string]*ws)
	for _, segmentMap := range w {
		fmt.Print("\nsegment has length: ")
		fmt.Println(len(segmentMap))
		for key, val := range segmentMap {
			if existing, found := combinedResults[key]; found {
				combinedResults[key] = merge(existing, val)
			} else {
				combinedResults[key] = val
			}
		}
	}

	sortAndPrint(&combinedResults)
	*/
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
			nextOffset++
			break
		}

		size += 1
		nextOffset++
	}

	return nextOffset + 1, int64(size)
}

func process(f *os.File, offset int64, size int64, segment int) map[string]*ws {
	fmt.Println("Segment %v size %v offset %v\n", segment, size, offset)
	csize := int64(2 * 16384)
	w := make(map[string]*ws)
	var leftover []byte

	endOffset := offset + size
	for offset < endOffset {
		chunkSize := csize
		if offset+chunkSize > endOffset {
			chunkSize = endOffset - offset
		}
		leftover = processChunk(f, offset, chunkSize, &w, leftover)
		offset += chunkSize
	}
	if len(leftover) > 0 {
		processLine(leftover, &w)
	}

	return w
}

func processChunk(
	f *os.File,
	offset int64,
	size int64,
	stations *map[string]*ws,
	leftover []byte,
) []byte {
	buf := make([]byte, size)
	n, err := f.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		panic(err)
	}

	buf = buf[:n]

	start := 0
	for i, ch := range buf {
		if ch == '\n' {
			line := append(leftover, buf[start:i+1]...)
			processLine(line, stations)
			leftover = nil
			start = i + 1
		}
	}

	if start < len(buf) {
		leftover = append(leftover, buf[start:]...)
	}

	return leftover
}

func processLine(line []byte, stations *map[string]*ws) {
	split := strings.Split(string(line), ";")
	if len(split) < 2 {
		return
	}
	str := strings.Replace(split[1], ".", "", 1)
	str = strings.TrimSuffix(str, "\n")
	temp, err := strconv.Atoi(str)
	if err != nil {
		panic(err)
	}

	name := split[0]
	val, ok := (*stations)[name]
	if ok {
		(*stations)[name] = val.PutTemp(int32(temp))
	} else {
		(*stations)[name] = NewWs(int32(temp))
	}
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
