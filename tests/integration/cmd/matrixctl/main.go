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

// Command matrixctl validates and queries integration profile selectors.
package main

import (
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"

	"go.yaml.in/yaml/v3"
)

type matrix struct {
	Groups []group `yaml:"groups"`
}

type group struct {
	Name      string   `yaml:"name"`
	Profiles  []string `yaml:"profiles"`
	Selectors []string `yaml:"selectors"`
}

func main() {
	if len(os.Args) < 2 {
		fail(errors.New("usage: matrixctl <check|regex|list> [args]"))
	}

	switch os.Args[1] {
	case "check":
		fail(runCheck())
	case "regex":
		fail(runRegex(os.Args[2:]))
	case "list":
		fail(runList(os.Args[2:]))
	default:
		fail(fmt.Errorf("unknown command %q", os.Args[1]))
	}
}

func runCheck() error {
	m, err := loadMatrix(detectMatrixPath())
	if err != nil {
		return err
	}

	tests, err := listIntegrationTests(detectIntegrationDir())
	if err != nil {
		return err
	}

	compiled, err := compileSelectors(m.Groups)
	if err != nil {
		return err
	}

	var uncovered []string
	for _, testName := range tests {
		matched := false
		for _, re := range compiled {
			if re.MatchString(testName) {
				matched = true
				break
			}
		}
		if !matched {
			uncovered = append(uncovered, testName)
		}
	}

	var deadSelectors []string
	for i, re := range compiled {
		hit := slices.ContainsFunc(tests, re.MatchString)
		if !hit {
			deadSelectors = append(deadSelectors, fmt.Sprintf("[%d] %s", i, re.String()))
		}
	}

	if len(uncovered) > 0 || len(deadSelectors) > 0 {
		var b strings.Builder
		if len(uncovered) > 0 {
			sort.Strings(uncovered)
			b.WriteString("matrix parity check failed: uncovered tests:\n")
			for _, t := range uncovered {
				b.WriteString("  - ")
				b.WriteString(t)
				b.WriteString("\n")
			}
		}
		if len(deadSelectors) > 0 {
			sort.Strings(deadSelectors)
			b.WriteString("matrix parity check failed: selectors with no matches:\n")
			for _, s := range deadSelectors {
				b.WriteString("  - ")
				b.WriteString(s)
				b.WriteString("\n")
			}
		}
		return errors.New(strings.TrimSpace(b.String()))
	}

	fmt.Printf("matrix parity check passed: %d tests covered\n", len(tests))
	return nil
}

func runRegex(args []string) error {
	fs := flag.NewFlagSet("regex", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	profile := fs.String("profile", "", "profile name from matrix.yaml")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *profile == "" {
		return errors.New("regex: --profile is required")
	}

	m, err := loadMatrix(detectMatrixPath())
	if err != nil {
		return err
	}

	selectors := selectorsForProfile(m.Groups, *profile)
	if len(selectors) == 0 {
		return fmt.Errorf("regex: no selectors for profile %q", *profile)
	}
	fmt.Println(strings.Join(selectors, "|"))
	return nil
}

func runList(args []string) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	profile := fs.String("profile", "", "profile name from matrix.yaml")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *profile == "" {
		return errors.New("list: --profile is required")
	}

	m, err := loadMatrix(detectMatrixPath())
	if err != nil {
		return err
	}
	tests, err := listIntegrationTests(detectIntegrationDir())
	if err != nil {
		return err
	}

	selectors := selectorsForProfile(m.Groups, *profile)
	if len(selectors) == 0 {
		return fmt.Errorf("list: no selectors for profile %q", *profile)
	}
	compiled, err := compileRawSelectors(selectors)
	if err != nil {
		return err
	}

	var matched []string
	for _, testName := range tests {
		for _, re := range compiled {
			if re.MatchString(testName) {
				matched = append(matched, testName)
				break
			}
		}
	}
	matched = uniqueStrings(matched)
	for _, t := range matched {
		fmt.Println(t)
	}
	fmt.Fprintf(os.Stderr, "profile %q matched %d tests\n", *profile, len(matched))
	return nil
}

func selectorsForProfile(groups []group, profile string) []string {
	var out []string
	for _, g := range groups {
		if !contains(g.Profiles, profile) {
			continue
		}
		out = append(out, g.Selectors...)
	}
	return uniqueStrings(out)
}

func compileSelectors(groups []group) ([]*regexp.Regexp, error) {
	var compiled []*regexp.Regexp
	for _, g := range groups {
		for _, s := range g.Selectors {
			re, err := regexp.Compile(s)
			if err != nil {
				return nil, fmt.Errorf("group %q invalid selector %q: %w", g.Name, s, err)
			}
			compiled = append(compiled, re)
		}
	}
	return compiled, nil
}

func compileRawSelectors(selectors []string) ([]*regexp.Regexp, error) {
	var compiled []*regexp.Regexp
	for _, s := range selectors {
		re, err := regexp.Compile(s)
		if err != nil {
			return nil, fmt.Errorf("invalid selector %q: %w", s, err)
		}
		compiled = append(compiled, re)
	}
	return compiled, nil
}

func loadMatrix(path string) (*matrix, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read matrix: %w", err)
	}
	var m matrix
	if err := yaml.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse matrix: %w", err)
	}
	if len(m.Groups) == 0 {
		return nil, errors.New("matrix has no groups")
	}
	return &m, nil
}

func detectMatrixPath() string {
	candidates := []string{
		"tests/integration/matrix.yaml",
		"integration/matrix.yaml",
		"matrix.yaml",
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	return "tests/integration/matrix.yaml"
}

func detectIntegrationDir() string {
	candidates := []string{
		"tests/integration",
		"integration",
		".",
	}
	for _, c := range candidates {
		fi, err := os.Stat(c)
		if err == nil && fi.IsDir() {
			return c
		}
	}
	return "tests/integration"
}

func listIntegrationTests(dir string) ([]string, error) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read integration dir: %w", err)
	}

	var tests []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, "_test.go") {
			continue
		}
		filePath := filepath.Join(dir, name)
		file, err := parser.ParseFile(fset, filePath, nil, 0)
		if err != nil {
			return nil, fmt.Errorf("parse integration test file %s: %w", filePath, err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || fn.Name == nil {
				continue
			}
			testName := fn.Name.Name
			if testName == "TestMain" || !strings.HasPrefix(testName, "Test") {
				continue
			}
			tests = append(tests, testName)
		}
	}

	if len(tests) == 0 {
		return nil, fmt.Errorf("no integration tests found in %s", filepath.Clean(dir))
	}
	sort.Strings(tests)
	return uniqueStrings(tests), nil
}

func uniqueStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	sort.Strings(in)
	out := []string{in[0]}
	for i := 1; i < len(in); i++ {
		if in[i] != in[i-1] {
			out = append(out, in[i])
		}
	}
	return out
}

func contains(values []string, target string) bool {
	return slices.Contains(values, target)
}

func fail(err error) {
	if err == nil {
		return
	}
	fmt.Fprintln(os.Stderr, err.Error())
	os.Exit(1)
}
