package main

import (
	"fmt"
	"github.com/AverageJoeWang/gBloomFilter"
)

func main()  {
	bf := gBloomFilter.NewBloomFilter(1000, 8, true)
	for i := 0; i <= 1000; i ++ {
		str1 := fmt.Sprintf("%s%0*d", "V425863",4, i)
		bf.AddString(str1)
	}

	for i := 0; i <= 1000; i ++ {
		str1 := fmt.Sprintf("%s%0*d", "V425863",4, i)
		if !bf.CheckString(str1) {
			fmt.Println(str1)
		}
	}

	fmt.Println(bf.Cap(), bf.FalsePositiveRate(), bf.KeySize())
	fmt.Println(bf)
}
