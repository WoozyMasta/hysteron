package health

import "testing"

func TestBuildListenerPlan(t *testing.T) {
	plan := BuildListenerPlan(map[RouteGroup]string{
		RouteGroupWeb:     "127.0.0.1:8080",
		RouteGroupMetrics: "127.0.0.1:9090",
		RouteGroupHealth:  "127.0.0.1:8080",
	})

	if len(plan) != 2 {
		t.Fatalf("expected 2 listeners, got %d", len(plan))
	}

	webGroups, ok := plan["127.0.0.1:8080"]
	if !ok {
		t.Fatalf("missing shared web listener")
	}
	if len(webGroups) != 2 || webGroups[0] != RouteGroupHealth || webGroups[1] != RouteGroupWeb {
		t.Fatalf("unexpected web groups: %v", webGroups)
	}

	metricsGroups, ok := plan["127.0.0.1:9090"]
	if !ok {
		t.Fatalf("missing metrics listener")
	}
	if len(metricsGroups) != 1 || metricsGroups[0] != RouteGroupMetrics {
		t.Fatalf("unexpected metrics groups: %v", metricsGroups)
	}
}

func TestBuildListenerPlanSkipsEmptyAddress(t *testing.T) {
	plan := BuildListenerPlan(map[RouteGroup]string{
		RouteGroupWeb:     "",
		RouteGroupMetrics: "127.0.0.1:9090",
		RouteGroupHealth:  "",
	})

	if len(plan) != 1 {
		t.Fatalf("expected 1 listener, got %d", len(plan))
	}
	groups := plan["127.0.0.1:9090"]
	if len(groups) != 1 || groups[0] != RouteGroupMetrics {
		t.Fatalf("unexpected groups: %v", groups)
	}
}
