// nsrun — NS-M4 嵌套挂起差分 runner（go 腿）：单进程执行一份用例——
// `Execute → Suspend → ResumeSuspension(…)…`——把统一形状的结果 JSON 打到
// stdout，供 tools/nested_suspend_diff/diffcheck.py 与 rs/plaita 两腿对拍。
//
// 用例/结果形状见 tools/nested_suspend_diff/README.md；拒绝只比类别
// （loop_frame/parallel_frame/resume_guard）——文案是引擎方言。
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/jeffkit/plaita-go/pkg/ir"
	"github.com/jeffkit/plaita-go/pkg/workflow"
)

// 私有订阅绑定键（go resumeBindingKey 同值；rs/plaita 同名键）：三仓都接受
// 该键承载绑定且准入后剥掉，恢复载荷不残留引擎方言。
const bindingKey = "__plaita_subscription_id"

// mapProvider 用例 flows 注册表：reference 节点的 flowId 在装载期经此解析
// （与 rs ns.rs 的 MapProvider、plaita 调度器注入同职能）。
type mapProvider struct {
	flows map[string][]byte
}

func (m mapProvider) LoadFlowJSON(flowID, _ string) ([]byte, error) {
	raw, ok := m.flows[flowID]
	if !ok {
		return nil, fmt.Errorf("case flow registry: flow %q not found", flowID)
	}
	return raw, nil
}

// seqRegistry 内存订阅注册表：顺序派发 sub-N（与三仓测试的假件同形）。
type seqRegistry struct {
	seq int
}

func (r *seqRegistry) Register(_ context.Context, req *workflow.SubscriptionRequest) error {
	r.seq++
	req.SubscriptionID = fmt.Sprintf("sub-%d", r.seq)
	return nil
}

func (r *seqRegistry) Unregister(_ context.Context, _, _ string) error { return nil }

type resumeStep struct {
	Type string         `json:"type"`
	Data map[string]any `json:"data"`
	Bind string         `json:"bind"`
}

type caseFile struct {
	Name   string                      `json:"name"`
	Flow   map[string]any              `json:"flow"`
	Flows  map[string]json.RawMessage  `json:"flows"`
	Input  json.RawMessage             `json:"input"`
	Resume []resumeStep                `json:"resume"`
}

// adaptReferenceKeys 把 reference 节点引用键从 plaita 生产者方言 flowID
// 改写为 go 装载层的 flowId（rs 同；差异已记 README「分歧清单」）。runner
// 侧改名，引擎装载路径保持原样。
func adaptReferenceKeys(v any) any {
	switch t := v.(type) {
	case map[string]any:
		if fid, ok := t["flowID"]; ok {
			if _, exists := t["flowId"]; !exists {
				t["flowId"] = fid
			}
			delete(t, "flowID")
		}
		for k, val := range t {
			t[k] = adaptReferenceKeys(val)
		}
		return t
	case []any:
		for i := range t {
			t[i] = adaptReferenceKeys(t[i])
		}
		return t
	default:
		return v
	}
}

func decodeJSON(raw []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

// rejectCategory 拒绝文案 → 具名类别（go 方言：loop 闸点名 loop-iteration、
// parallel 静态闸点名 contains a suspending node、绑定守卫点名 not bound
// to the suspended subscription）。
func rejectCategory(message string) string {
	switch {
	case strings.Contains(message, "loop-iteration"):
		return "loop_frame"
	case strings.Contains(message, "contains a suspending node"):
		return "parallel_frame"
	case strings.Contains(message, "not bound to the suspended subscription"),
		strings.Contains(message, "event type mismatch"):
		return "resume_guard"
	default:
		return "error"
	}
}

// carrierSubID 从挂起现场的 pending 节点态读载体登记的订阅 id（go 测试
// nsCarrierSubID 同款路径）。
func carrierSubID(susp *workflow.SuspendError) string {
	nodes, _ := susp.Checkpoint.Context["$NODE"].(map[string]any)
	state, _ := nodes[string(susp.NodeID)].(map[string]any)
	id, _ := state["subscription_id"].(string)
	return id
}

func emit(result map[string]any) {
	out, err := json.Marshal(result)
	if err != nil {
		fmt.Fprintf(os.Stderr, "nsrun: marshal result: %v\n", err)
		os.Exit(2)
	}
	fmt.Println(string(out))
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: nsrun <case.json>")
		os.Exit(2)
	}
	raw, err := os.ReadFile(os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "nsrun: read case: %v\n", err)
		os.Exit(2)
	}
	var c caseFile
	if err := json.Unmarshal(raw, &c); err != nil {
		fmt.Fprintf(os.Stderr, "nsrun: parse case: %v\n", err)
		os.Exit(2)
	}

	flowAny, err := decodeJSON(raw)
	if err == nil {
		if m, ok := flowAny.(map[string]any); ok {
			if f, ok := m["flow"].(map[string]any); ok {
				adaptReferenceKeys(f)
				c.Flow = f
			}
		}
	}
	flowBytes, err := json.Marshal(c.Flow)
	if err != nil {
		fmt.Fprintf(os.Stderr, "nsrun: render flow: %v\n", err)
		os.Exit(2)
	}

	flows := map[string][]byte{}
	for id, doc := range c.Flows {
		var v any
		if v, err = decodeJSON(doc); err == nil {
			adaptReferenceKeys(v)
			flows[id], err = json.Marshal(v)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "nsrun: flow %q: %v\n", id, err)
			os.Exit(2)
		}
	}

	wf, err := ir.LoadWithFlowProvider(flowBytes, mapProvider{flows: flows})
	if err != nil {
		emit(map[string]any{"name": c.Name, "outcome": "engine_error", "detail": fmt.Sprintf("load flow: %v", err)})
		return
	}

	reg := &seqRegistry{}
	ctx := workflow.WithSuspendOptions(context.Background(), &workflow.SuspendOptions{
		ExecutionID:   "exec-ns",
		TenantID:      "t1",
		FlowID:        string(wf.ID),
		Store:         workflow.NewMemoryCheckpointStore(),
		Subscriptions: reg,
	})

	var input any = map[string]any{}
	if len(c.Input) > 0 {
		if input, err = decodeJSON(c.Input); err != nil {
			fmt.Fprintf(os.Stderr, "nsrun: parse input: %v\n", err)
			os.Exit(2)
		}
	}

	// resume 步序：顺序消费（index 游标）。
	next := 0
	suspendedNodes := []string{}
	stillSuspended := false

	result, err := wf.Execute(ctx, input)
	for {
		if err == nil {
			break
		}
		susp := workflow.AsSuspend(err)
		if susp == nil {
			emit(map[string]any{
				"name":            c.Name,
				"outcome":         "rejected",
				"suspended_node":  firstOrNil(suspendedNodes),
				"suspended_nodes": suspendedNodes,
				"reject_reason":   rejectCategory(err.Error()),
				"detail":          err.Error(),
			})
			return
		}
		suspendedNodes = append(suspendedNodes, string(susp.NodeID))
		if next >= len(c.Resume) {
			stillSuspended = true
			break
		}
		step := c.Resume[next]
		next++
		data := map[string]any{}
		for k, v := range step.Data {
			data[k] = v
		}
		if step.Bind == "wrong" {
			data[bindingKey] = "sub-wrong"
		} else {
			data[bindingKey] = carrierSubID(susp)
		}
		result, err = wf.ResumeSuspension(ctx, susp, step.Type, data)
	}

	if stillSuspended {
		emit(map[string]any{
			"name":            c.Name,
			"outcome":         "suspended",
			"suspended_node":  firstOrNil(suspendedNodes),
			"suspended_nodes": suspendedNodes,
		})
		return
	}
	if err != nil {
		emit(map[string]any{
			"name":            c.Name,
			"outcome":         "rejected",
			"suspended_node":  firstOrNil(suspendedNodes),
			"suspended_nodes": suspendedNodes,
			"reject_reason":   rejectCategory(err.Error()),
			"detail":          err.Error(),
		})
		return
	}
	emit(map[string]any{
		"name":            c.Name,
		"outcome":         "completed",
		"suspended_node":  firstOrNil(suspendedNodes),
		"suspended_nodes": suspendedNodes,
		"output":          result,
	})
}

func firstOrNil(nodes []string) any {
	if len(nodes) == 0 {
		return nil
	}
	return nodes[0]
}
