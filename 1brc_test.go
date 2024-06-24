package main

import "testing"

func Benchmark1brc(b *testing.B) {
	for n := 0; n < b.N; n++ {
		main()
	}
}
