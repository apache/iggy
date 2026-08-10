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
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Iggy.Utils;

namespace Apache.Iggy.Tests.UtilityTests;

public class SlicedMemoryOwnerTests
{
    private const int BufferSize = 4096;

    [Fact]
    public void Dispose_ReturnsBufferToPool()
    {
        var owner = ArrayPoolHelper.Rent(BufferSize);
        var underlying = GetUnderlyingArray(owner);
        owner.Dispose();

        using var second = ArrayPoolHelper.Rent(BufferSize);
        Assert.Same(underlying, GetUnderlyingArray(second));
    }

    [Fact]
    public void Dispose_IsIdempotent()
    {
        var owner = ArrayPoolHelper.Rent(BufferSize);
        owner.Dispose();
        owner.Dispose();
        owner.Dispose();
    }

    [Fact]
    public void FinalizerIsDeclared()
    {
        var sliced = typeof(ArrayPoolHelper)
            .GetNestedType("SlicedMemoryOwner", BindingFlags.NonPublic)!;

        var finalizer = sliced.GetMethod("Finalize", BindingFlags.NonPublic | BindingFlags.Instance);

        Assert.NotNull(finalizer);
    }

    [Fact]
    public void ForgotDispose_FinalizerRunsAndReclaimsInstance()
    {
        var weakRef = RentWeak();

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        Assert.False(weakRef.IsAlive);

        [MethodImpl(MethodImplOptions.NoInlining)]
        static WeakReference RentWeak()
        {
            return new WeakReference(ArrayPoolHelper.Rent(BufferSize));
        }
    }

    private static byte[] GetUnderlyingArray(IMemoryOwner<byte> owner)
    {
        if (!MemoryMarshal.TryGetArray<byte>(owner.Memory, out ArraySegment<byte> segment) || segment.Array is null)
        {
            throw new InvalidOperationException("SlicedMemoryOwner.Memory must be array-backed.");
        }

        return segment.Array;
    }
}
