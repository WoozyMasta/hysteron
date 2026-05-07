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

package commands

import "errors"

var (
	// ErrNoActiveCommand reports a parse outcome with no selected command.
	ErrNoActiveCommand = errors.New("no active command selected")
	// ErrRuntimeCommandContextMissing reports missing runtime parent context.
	ErrRuntimeCommandContextMissing = errors.New("runtime command context is missing")
	// ErrInvalidListenEndpoint reports invalid host:port listen endpoint values.
	ErrInvalidListenEndpoint = errors.New("invalid listen endpoint")
	// ErrCommandInputRequired reports missing command payload input.
	ErrCommandInputRequired = errors.New("no input provided")
	// ErrCommandInputConflict reports conflicting file and positional payload.
	ErrCommandInputConflict = errors.New("only one of positional input or --file can be provided")
	// ErrTooManyCommandArguments reports unexpected extra positional arguments.
	ErrTooManyCommandArguments = errors.New("too many arguments")
)
