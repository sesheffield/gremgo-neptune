package gremutil

import (
	"fmt"

	"github.com/davecgh/go-spew/spew"
)

const ColEnd = "\x1b[0m"
const Verbose = 1

func col(n string) string {
	return "\x1b[" + n + "m"
}

func Dump(ind string, data ...interface{}) {
	DumpLev(2, ind, data...)
}
func DumpLev(lev int, ind string, data ...interface{}) {
	if Verbose < lev {
		return
	}
	var scs = spew.ConfigState{Indent: ind, HighlightValues: true}
	// scs.Indent = ind
	fmt.Print(col("34;1"), ind, ColEnd)
	scs.Dump(data...)
}

func Warn(fmat string, args ...interface{}) {
	WarnLev(1, fmat, args...)
}
func WarnLev(lev int, fmat string, args ...interface{}) {
	if Verbose < lev {
		return
	}
	fmt.Printf(col("36;1")+"Warn: "+col("0;36")+fmat+ColEnd+"\n", args...)
}

func Info(fmat string, args ...interface{}) {
	InfoLev(3, fmat, args...)
}
func InfoLev(lev int, fmat string, args ...interface{}) {
	if Verbose < lev {
		return
	}
	fmt.Printf("\t  "+col("34")+fmat+ColEnd+"\n", args...)
}

func InfoPath(path []string, fmat string, args ...interface{}) {
	InfoPathLev(3, path, fmat, args...)
}
func InfoPathLev(lev int, path []string, fmat string, args ...interface{}) {
	if Verbose < lev {
		return
	}
	fmt.Printf("\t  "+col("34")+"%-100s %v"+ColEnd+"\n", fmt.Sprintf(fmat, args...), path)
	// fmt.Printf("\t\t\x1b[34m"+fmat+colEnd+"\n", args...)
}
