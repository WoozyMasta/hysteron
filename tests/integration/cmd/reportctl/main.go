// Copyright 2026 WoozyMasta
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
// See the License for the specific language governing permissions and
// limitations under the License.

// Command reportctl renders integration test reports from go test JSON output.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type goTestEvent struct {
	Time    time.Time `json:"Time"`
	Action  string    `json:"Action"`
	Package string    `json:"Package"`
	Test    string    `json:"Test"`
	Output  string    `json:"Output"`
	Elapsed float64   `json:"Elapsed"`
}

type testResult struct {
	Name          string
	Status        string
	Elapsed       float64
	FailureOutput []string
}

func main() {
	if len(os.Args) < 2 {
		fail(errors.New("usage: reportctl <markdown|index> [args]"))
	}
	switch os.Args[1] {
	case "markdown":
		fail(runMarkdown(os.Args[2:]))
	case "index":
		fail(runIndex(os.Args[2:]))
	default:
		fail(fmt.Errorf("unknown command %q", os.Args[1]))
	}
}

func runMarkdown(args []string) error {
	fs := flag.NewFlagSet("markdown", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	input := fs.String("input", "", "path to go test -json output file")
	output := fs.String("output", "", "path to markdown output file")
	profile := fs.String("profile", "", "integration profile name")
	pgMajor := fs.String("pg-major", "", "postgres major version")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *input == "" || *output == "" {
		return errors.New("markdown: --input and --output are required")
	}

	results, pkgStatus, pkgElapsed, err := parseResults(*input)
	if err != nil {
		return err
	}
	md := renderMarkdown(results, pkgStatus, pkgElapsed, *profile, *pgMajor, *input)
	if err := os.WriteFile(*output, []byte(md), 0600); err != nil {
		return fmt.Errorf("write markdown report: %w", err)
	}
	fmt.Printf("wrote markdown report: %s\n", *output)
	return nil
}

type reportIndexEntry struct {
	Profile       string
	PGMajor       string
	Timestamp     string
	PackageStatus string
	TestsTotal    int
	TestsFailed   int
	MDPath        string
	JSONPath      string
}

func runIndex(args []string) error {
	fs := flag.NewFlagSet("index", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	artifactsDir := fs.String("artifacts-dir", "artifacts/integration", "path to integration artifacts directory")
	output := fs.String("output", "", "path to markdown index output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *output == "" {
		return errors.New("index: --output is required")
	}

	entries, err := collectLatestReports(*artifactsDir)
	if err != nil {
		return err
	}
	md := renderIndexMarkdown(entries, *artifactsDir)
	if err := os.WriteFile(*output, []byte(md), 0600); err != nil {
		return fmt.Errorf("write index markdown: %w", err)
	}
	fmt.Printf("wrote report index: %s\n", *output)
	return nil
}

func parseResults(path string) (_ []testResult, _ string, _ float64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, "", 0, fmt.Errorf("open input: %w", err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close input: %w", closeErr)
		}
	}()

	resultsMap := map[string]testResult{}
	outputs := map[string][]string{}
	pkgStatus := "unknown"
	var pkgElapsed float64

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var ev goTestEvent
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			// Ignore non-json lines to remain robust on mixed logs.
			continue
		}
		if ev.Test != "" {
			if ev.Action == "output" {
				line := strings.TrimSpace(ev.Output)
				if line != "" {
					outputs[ev.Test] = append(outputs[ev.Test], line)
				}
			}
			if ev.Action == "pass" || ev.Action == "fail" || ev.Action == "skip" {
				resultsMap[ev.Test] = testResult{
					Name:          ev.Test,
					Status:        ev.Action,
					Elapsed:       ev.Elapsed,
					FailureOutput: nil,
				}
			}
		} else {
			if ev.Action == "pass" || ev.Action == "fail" {
				pkgStatus = ev.Action
				pkgElapsed = ev.Elapsed
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, "", 0, fmt.Errorf("scan input: %w", err)
	}

	results := make([]testResult, 0, len(resultsMap))
	for name, r := range resultsMap {
		if r.Status == "fail" {
			r.FailureOutput = tailMeaningfulLines(outputs[name], 12)
		}
		results = append(results, r)
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].Status != results[j].Status {
			// failed first, then skipped, then passed
			weight := map[string]int{"fail": 0, "skip": 1, "pass": 2}
			return weight[results[i].Status] < weight[results[j].Status]
		}
		return results[i].Name < results[j].Name
	})
	return results, pkgStatus, pkgElapsed, nil
}

func tailMeaningfulLines(lines []string, n int) []string {
	if len(lines) == 0 || n <= 0 {
		return nil
	}
	filtered := make([]string, 0, len(lines))
	for _, line := range lines {
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "=== RUN") ||
			strings.HasPrefix(line, "=== PAUSE") ||
			strings.HasPrefix(line, "=== CONT") {
			continue
		}
		filtered = append(filtered, line)
	}
	if len(filtered) == 0 {
		return nil
	}
	if len(filtered) <= n {
		return filtered
	}
	return filtered[len(filtered)-n:]
}

var reportFileRe = regexp.MustCompile(`^integration-run-([A-Za-z0-9_-]+)-pg([0-9]+)-([0-9T]+Z)\.md$`)

func collectLatestReports(artifactsDir string) ([]reportIndexEntry, error) {
	latest := map[string]reportIndexEntry{}
	err := filepath.WalkDir(artifactsDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		base := filepath.Base(path)
		m := reportFileRe.FindStringSubmatch(base)
		if len(m) != 4 {
			return nil
		}
		profile, pgMajor, timestamp := m[1], m[2], m[3]
		key := profile + "|pg" + pgMajor
		entry := reportIndexEntry{
			Profile:   profile,
			PGMajor:   pgMajor,
			Timestamp: timestamp,
			MDPath:    path,
			JSONPath: filepath.Join(
				filepath.Dir(path),
				fmt.Sprintf("integration-run-%s-pg%s-%s.jsonl", profile, pgMajor, timestamp),
			),
		}
		populateReportSummary(&entry)
		prev, ok := latest[key]
		if !ok || entry.Timestamp > prev.Timestamp {
			latest[key] = entry
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scan artifacts dir: %w", err)
	}

	out := make([]reportIndexEntry, 0, len(latest))
	for _, entry := range latest {
		out = append(out, entry)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Profile != out[j].Profile {
			return out[i].Profile < out[j].Profile
		}
		a, _ := strconv.Atoi(out[i].PGMajor)
		b, _ := strconv.Atoi(out[j].PGMajor)
		if a != b {
			return a > b
		}
		return out[i].Timestamp > out[j].Timestamp
	})
	return out, nil
}

func populateReportSummary(entry *reportIndexEntry) {
	data, err := os.ReadFile(entry.MDPath)
	if err != nil {
		return
	}
	text := string(data)
	entry.PackageStatus = extractBacktickedValue(text, "package_status")
	entry.TestsTotal = extractBacktickedInt(text, "tests_total")
	entry.TestsFailed = extractBacktickedInt(text, "tests_failed")
}

func extractBacktickedValue(text string, field string) string {
	prefix := "* " + field + ": `"
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		v := strings.TrimPrefix(line, prefix)
		v = strings.TrimSuffix(v, "`")
		return v
	}
	return ""
}

func extractBacktickedInt(text string, field string) int {
	v := extractBacktickedValue(text, field)
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0
	}
	return n
}

func renderMarkdown(results []testResult, pkgStatus string, pkgElapsed float64, profile, pgMajor, input string) string {
	var pass, fail, skip int
	for _, r := range results {
		switch r.Status {
		case "pass":
			pass++
		case "fail":
			fail++
		case "skip":
			skip++
		}
	}

	var b strings.Builder
	b.WriteString("# Integration Test Report\n\n")
	if profile != "" || pgMajor != "" {
		b.WriteString("## Context\n\n")
		if profile != "" {
			fmt.Fprintf(&b, "* profile: `%s`\n", profile)
		}
		if pgMajor != "" {
			fmt.Fprintf(&b, "* pg_major: `%s`\n", pgMajor)
		}
		b.WriteString("\n")
	}

	b.WriteString("## Summary\n\n")
	fmt.Fprintf(&b, "* package_status: `%s`\n", pkgStatus)
	fmt.Fprintf(&b, "* package_elapsed: `%.3fs`\n", pkgElapsed)
	fmt.Fprintf(&b, "* tests_total: `%d`\n", len(results))
	fmt.Fprintf(&b, "* tests_passed: `%d`\n", pass)
	fmt.Fprintf(&b, "* tests_failed: `%d`\n", fail)
	fmt.Fprintf(&b, "* tests_skipped: `%d`\n", skip)
	fmt.Fprintf(&b, "* source_json: `%s`\n\n", input)

	b.WriteString("## Tests\n\n")
	b.WriteString("| test | status | elapsed_s |\n")
	b.WriteString("|---|---|---:|\n")
	for _, r := range results {
		fmt.Fprintf(&b, "| `%s` | `%s` | %.3f |\n", r.Name, r.Status, r.Elapsed)
	}

	if fail > 0 {
		b.WriteString("\n## Failures\n\n")
		for _, r := range results {
			if r.Status != "fail" {
				continue
			}
			fmt.Fprintf(&b, "### `%s`\n\n", r.Name)
			if len(r.FailureOutput) == 0 {
				b.WriteString("No captured failure output.\n\n")
				continue
			}
			b.WriteString("```text\n")
			for _, line := range r.FailureOutput {
				b.WriteString(line)
				b.WriteString("\n")
			}
			b.WriteString("```\n\n")
		}
	}
	return b.String()
}

func renderIndexMarkdown(entries []reportIndexEntry, artifactsDir string) string {
	var b strings.Builder
	b.WriteString("# Integration Report Index\n\n")
	fmt.Fprintf(&b, "* generated_at: `%s`\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&b, "* artifacts_dir: `%s`\n\n", artifactsDir)
	if len(entries) == 0 {
		b.WriteString("No integration markdown reports found.\n")
		return b.String()
	}

	b.WriteString("| profile | pg_major | package_status | tests_total | tests_failed | timestamp | markdown | json |\n")
	b.WriteString("|---|---:|---|---:|---:|---|---|---|\n")
	for _, e := range entries {
		mdPath := filepath.ToSlash(e.MDPath)
		jsonPath := filepath.ToSlash(e.JSONPath)
		fmt.Fprintf(
			&b,
			"| `%s` | `%s` | `%s` | %d | %d | `%s` | `%s` | `%s` |\n",
			e.Profile,
			e.PGMajor,
			e.PackageStatus,
			e.TestsTotal,
			e.TestsFailed,
			e.Timestamp,
			mdPath,
			jsonPath,
		)
	}
	return b.String()
}

func fail(err error) {
	if err == nil {
		return
	}
	fmt.Fprintln(os.Stderr, err.Error())
	os.Exit(1)
}
