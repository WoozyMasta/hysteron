// Copyright 2016 Sorint.lab
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

//go:build linux
// +build linux

package timer

import (
	"syscall"
	"unsafe"
)

const (
	// from /usr/include/linux/time.h
	CLOCK_MONOTONIC = 1
)

// Use a syscall so this works consistently across Linux architectures.
// This can be slower than vDSO, but this call is not performance-critical.
func Now() int64 {
	var ts syscall.Timespec
	_, _, _ = syscall.Syscall(syscall.SYS_CLOCK_GETTIME, CLOCK_MONOTONIC, uintptr(unsafe.Pointer(&ts)), 0)
	nsec := ts.Nano()
	return nsec
}
