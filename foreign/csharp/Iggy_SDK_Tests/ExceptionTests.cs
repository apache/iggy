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

using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;

namespace Apache.Iggy.Tests;

public class ExceptionTests
{
    [Fact]
    public void Exception_ShouldHaveCorrectErrorCodeAndMessage()
    {
        var exception = new IggyInvalidStatusCodeException((int)IggyErrorCode.CannotCreateSegmentTimeIndexFile);

        Assert.Equal(4005, exception.StatusCode);
        Assert.Equal(IggyErrorCode.CannotCreateSegmentTimeIndexFile, exception.ErrorCode);
        Assert.Equal("Invalid status code: 4005 (CannotCreateSegmentTimeIndexFile)", exception.Message);
    }

    [Fact]
    public void Exception_ShouldHaveGenericError_When_ErrorIsOutOfRange()
    {
        var exception = new IggyInvalidStatusCodeException(9999999);

        Assert.Equal(9999999, exception.StatusCode);
        Assert.Equal(IggyErrorCode.Error, exception.ErrorCode);
        Assert.Equal("Invalid status code: 9999999 (Error)", exception.Message);
    }

    [Fact]
    public void Exception_ShouldBeAlreadyExistsErrorCode()
    {
        var exception = new IggyInvalidStatusCodeException((int)IggyErrorCode.StreamNameAlreadyExists);

        Assert.True(exception.IsAlreadyExists);
        Assert.False(exception.IsConnectionError);
        Assert.False(exception.IsNotFound);
        Assert.False(exception.IsAuthenticationError);
    }

    [Fact]
    public void Exception_ShouldBeAuthenticationErrorCode()
    {
        var exception = new IggyInvalidStatusCodeException((int)IggyErrorCode.Unauthenticated);

        Assert.True(exception.IsAuthenticationError);
    }

    [Fact]
    public void Exception_ShouldBeNotFoundErrorCode()
    {
        var exception = new IggyInvalidStatusCodeException((int)IggyErrorCode.StreamIdNotFound);

        Assert.True(exception.IsNotFound);
    }

    [Fact]
    public void Exception_ShouldBeConnectionErrorCode()
    {
        var exception = new IggyInvalidStatusCodeException((int)IggyErrorCode.TcpError);

        Assert.True(exception.IsConnectionError);
    }
}
