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

using System.Buffers;

namespace Apache.Iggy.Utils;

internal static class ArrayPoolHelper
{
    public static SlicedMemoryOwner Rent(int minimumLength, bool clearOnReturn = false)
    {
        return new SlicedMemoryOwner(minimumLength, clearOnReturn);
    }

    internal sealed class SlicedMemoryOwner(int minimumLength, bool clearOnReturn = false) : IMemoryOwner<byte>
    {
        private readonly byte[] _value = ArrayPool<byte>.Shared.Rent(minimumLength);
        private int _disposed;

        public Memory<byte> Memory => _value.AsMemory()[..minimumLength];

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            ArrayPool<byte>.Shared.Return(_value, clearOnReturn);
        }
    }
}
