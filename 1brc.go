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

func (w *ws) PutTemp(temp int32) {
	if temp < w.Min {
		w.Min = temp
	}
	if temp > w.Max {
		w.Max = temp
	}
	w.Sum = w.Sum + temp
	w.Count = w.Count + 1
}

func (w *ws) Merge(x *ws) {
	if x.Min < w.Min {
		w.Min = x.Min
	}
	if x.Max > w.Max {
		w.Max = x.Max
	}
	w.Count = w.Count + x.Count
	w.Sum = w.Sum + x.Sum
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
			w[i] = process(f, offset, size)
		}(f, offset, segmentSize, i)
		offset = nextOffset
	}

	wg.Wait()
	merged := make(map[string]*ws)
	for _, segmentMap := range w {
		for key, val := range segmentMap {
			if existing, found := merged[key]; found {
				existing.Merge(val)
			} else {
				merged[key] = val
			}
		}
	}
	sortAndPrint(&merged)
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
		_, err := f.ReadAt(b, nextOffset)
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
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

func process(f *os.File, offset int64, size int64) map[string]*ws {
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
		processLine(line, &w)
	}

	return w
}

func processLine(line []byte, w *map[string]*ws) {
	i := bytes.IndexByte(line, ';')

	name := string(line[:i])
	start := i + 1

	temp := bytesToInt16(line[start : len(line)-1])

	val, ok := (*w)[name]
	if ok {
		val.PutTemp(int32(temp))
	} else {
		(*w)[name] = NewWs(int32(temp))
	}
}

func bytesToInt16(b []byte) int16 {
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
		result = result*10 + int16(digit-'0')
	}

	if negative {
		result = -result
	}

	return result
}

func sortAndPrint(w *map[string]*ws) {
	keys := make([]string, 0, len(*w))
	for key := range *w {
		keys = append(keys, key)
	}

	skeys := sort.StringSlice(keys)
	skeys.Sort()

	fmt.Printf("{%v", (*w)[skeys[0]].String(skeys[0]))
	for i, key := range skeys {
		if i == 0 {
			continue
		}
		fmt.Printf(", ")
		w := (*w)[key]
		fmt.Print(w.String(key))
	}
	fmt.Printf("}\n")
}
