package workflow

import (
	"errors"
	"testing"
)

func TestSubmit_MissingDependency(t *testing.T) {
	e := newTestEngine(t)
	err := e.Submit("wf-missing", []*Task{
		okTask("a"),
		okTask("b", "a"),
		okTask("c", "a", "ghost"), // ghost 从未提交
	})
	ve := asValidationError(t, err)
	if ve.Kind != KindMissingDependency {
		t.Fatalf("Kind = %s, want %s", ve.Kind, KindMissingDependency)
	}
	if ve.TaskID != "c" || ve.DepID != "ghost" {
		t.Fatalf("error locations = task=%q dep=%q, want c/ghost", ve.TaskID, ve.DepID)
	}
	// 校验失败：工作流不得被登记，之后也无法触发。
	if _, err := e.Trigger(nil, "wf-missing", 1); !errors.Is(err, ErrWorkflowNotFound) {
		t.Fatalf("trigger after rejected submit should fail with ErrWorkflowNotFound, got %v", err)
	}
	t.Logf("missing dependency rejected distinctly: %v", ve)
}

func TestSubmit_CycleDependency(t *testing.T) {
	e := newTestEngine(t)
	err := e.Submit("wf-cycle", []*Task{
		okTask("a", "c"),
		okTask("b", "a"),
		okTask("c", "b"),
	})
	ve := asValidationError(t, err)
	if ve.Kind != KindCycleDependency {
		t.Fatalf("Kind = %s, want %s; detail: %s", ve.Kind, KindCycleDependency, ve.Detail)
	}
	// 被指出的任务必须在环上。
	if ve.TaskID != "a" && ve.TaskID != "b" && ve.TaskID != "c" {
		t.Fatalf("reported task %q is not on the cycle", ve.TaskID)
	}
	t.Logf("cycle rejected distinctly: task on cycle=%q detail=%s", ve.TaskID, ve.Detail)
}

func TestSubmit_SelfDependencyIsCycle(t *testing.T) {
	e := newTestEngine(t)
	err := e.Submit("wf-self", []*Task{okTask("x", "x")})
	ve := asValidationError(t, err)
	if ve.Kind != KindCycleDependency || ve.TaskID != "x" {
		t.Fatalf("self dependency: kind=%s task=%q, want cycle/x", ve.Kind, ve.TaskID)
	}
}

func TestSubmit_DuplicateTask(t *testing.T) {
	e := newTestEngine(t)
	err := e.Submit("wf-dup", []*Task{okTask("a"), okTask("a")})
	ve := asValidationError(t, err)
	if ve.Kind != KindDuplicateTask || ve.TaskID != "a" {
		t.Fatalf("kind=%s task=%q, want duplicate_task/a", ve.Kind, ve.TaskID)
	}
}

func TestSubmit_RejectReasonsAreDistinguishable(t *testing.T) {
	e := newTestEngine(t)
	kinds := map[ErrorKind]bool{}
	collect := func(err error) {
		ve := asValidationError(t, err)
		if kinds[ve.Kind] {
			t.Fatalf("kind %s observed twice", ve.Kind)
		}
		kinds[ve.Kind] = true
	}
	collect(e.Submit("w1", []*Task{okTask("a"), okTask("a")}))
	collect(e.Submit("w2", []*Task{okTask("a", "missing")}))
	collect(e.Submit("w3", []*Task{okTask("a", "b"), okTask("b", "a")}))
	for _, want := range []ErrorKind{KindDuplicateTask, KindMissingDependency, KindCycleDependency} {
		if !kinds[want] {
			t.Fatalf("kind %s not observed", want)
		}
	}
}
