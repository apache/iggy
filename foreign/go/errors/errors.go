// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package ierror

import (
	"errors"
	"fmt"
)

type IggyError struct {
	Code    ErrCode
	Message string
}

type Option func(err *IggyError)

func WithMsg(msg string) Option {
	return func(err *IggyError) {
		err.Message = msg
	}
}

// New returns a new IggyError
func New(code ErrCode, opts ...Option) *IggyError {
	err := &IggyError{
		Code: code,
	}
	for _, opt := range opts {
		opt(err)
	}
	return err
}

// IggyError implements the error interface.
func (e *IggyError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("code=%d: %s", e.Code, e.Code.String())
	}
	return fmt.Sprintf("code=%d: %s", e.Code, e.Message)
}

func (e *IggyError) Is(target error) bool {
	if t := new(IggyError); errors.As(target, &t) {
		return t.Code == e.Code
	}
	return false
}

// Code returns the error code for an error
// It supports wrapped error
func Code(err error) ErrCode {
	if err == nil {
		return OK
	}
	return FromError(err).Code
}

// FromError parse error to IggyError
// It supports wrapped error
func FromError(err error) *IggyError {
	if err == nil {
		return nil
	}
	if se := new(IggyError); errors.As(err, &se) {
		return se
	}
	return New(Error, WithMsg(err.Error()))
}
