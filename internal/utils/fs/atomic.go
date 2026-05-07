// Copyright 2015 Sorint.lab
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

package fs

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// WriteFileAtomicFunc atomically writes a file by writing to a temporary file
// and then renaming it to the destination path.
//
// This function is adapted from
// https://github.com/youtube/vitess/blob/master/go/ioutil2/ioutil.go
// (Copyright 2012, Google Inc. BSD-3-Clause).
func WriteFileAtomicFunc(
	filename string,
	perm os.FileMode,
	writeFunc func(f io.Writer) error,
) error {
	dir, name := filepath.Split(filename)
	f, err := os.CreateTemp(dir, name)
	if err != nil {
		return err
	}
	err = writeFunc(f)
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if permErr := os.Chmod(f.Name(), perm); err == nil {
		err = permErr
	}
	if err == nil {
		err = os.Rename(f.Name(), filename)
	}
	// Any err should result in full cleanup.
	if err != nil {
		if removeErr := os.Remove(f.Name()); removeErr != nil {
			return errors.Join(
				err,
				fmt.Errorf("remove temporary file %q: %w", f.Name(), removeErr),
			)
		}
	}
	return err
}

// WriteFileAtomic atomically writes a file.
func WriteFileAtomic(filename string, perm os.FileMode, data []byte) error {
	return WriteFileAtomicFunc(filename, perm, func(f io.Writer) error {
		_, err := f.Write(data)
		return err
	})
}
