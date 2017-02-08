package awsdynamodb

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Utility to print out the Item map (AttributeValues) with indentation.
//
func printDocItems(name string, m map[string]*dynamodb.AttributeValue, indentStart int) {
	var indent = [...]string{"", "\t", "\t\t", "\t\t\t", "\t\t\t\t", "\t\t\t\t\t", "\t\t\t\t\t\t",
		"\t\t\t\t\t\t\t", "\t\t\t\t\t\t\t\t", "\t\t\t\t\t\t\t\t\t"}
	var lvl int

	switch  {
	case indentStart < 1: lvl = 0
	case indentStart >= len(indent): lvl = len(indent) - 1
	default: lvl = 0
	}

	fmt.Printf("%s\"%s\" {\n", indent[lvl], name); lvl++
	for k, v := range m {
		if len(v.B) > 0 {
			fmt.Printf("%s%s {B:%v}\n", indent[lvl], k, v.B)
		}
		if len(v.BS) > 0 {
			fmt.Printf("%s%s {BS:<not printed>\n", indent[lvl], k)
		}
		if v.BOOL != nil {
			fmt.Printf("%s%s {BOOL:%v}\n", indent[lvl], k, *v.BOOL)
		}

		if v.N != nil {
			fmt.Printf("%s%s {N:%v}\n", indent[lvl], k, *v.N)
		}
		if len(v.NS) > 0 {
			fmt.Printf("%s%s {NS:[", indent[lvl], k)
			for i := range v.NS {
				fmt.Printf("%s ", *v.NS[i])
			}
			fmt.Println("]}")
		}
		if v.S != nil {
			fmt.Printf("%s%s: {S:%v}\n", indent[lvl], k, *v.S)
		}
		if len(v.SS) > 0 {
			fmt.Printf("%s%s {SS:[", indent[lvl], k)
			for i := range v.SS {
				fmt.Printf("\"%s\" ", *v.SS[i])
			}
			fmt.Println("]}")
		}
		if v.NULL != nil {
			fmt.Printf("%s%s {NULL:%v}", indent[lvl], k, *v.NULL)
		}
		if len(v.L) > 0 {
			fmt.Printf("%s%s {L:[%v]}", indent[lvl], k, v.L)
		}
		if len(v.M) > 0 {
			printDocItems(k, v.M, lvl)
		}

	}
	fmt.Println("}")
}
