// go-ref runs a deployment flow through plaita-go's engine and prints the
// JSON result — the reference side of the plaita-rs differential harness.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/jeffkit/plaita-go/pkg/ir"
)

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "go-ref: "+format+"\n", args...)
	os.Exit(2)
}

func main() {
	flowPath := flag.String("flow", "", "flow json path")
	inputPath := flag.String("input", "", "input json path (optional)")
	flag.Parse()
	if *flowPath == "" {
		fail("-flow is required")
	}
	data, err := os.ReadFile(*flowPath)
	if err != nil {
		fail("read flow: %v", err)
	}
	wf, err := ir.Load(data)
	if err != nil {
		fail("load: %v", err)
	}
	var input any = map[string]any{}
	if *inputPath != "" {
		b, err := os.ReadFile(*inputPath)
		if err != nil {
			fail("read input: %v", err)
		}
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.UseNumber()
		if err := dec.Decode(&input); err != nil {
			fail("parse input: %v", err)
		}
	}
	result, err := wf.Execute(context.Background(), input)
	if err != nil {
		fail("execute: %v", err)
	}
	out, err := json.Marshal(result)
	if err != nil {
		fail("marshal: %v", err)
	}
	fmt.Println(string(out))
}
