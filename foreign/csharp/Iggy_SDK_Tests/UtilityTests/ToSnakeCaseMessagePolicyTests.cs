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

using System.Collections;
using Apache.Iggy.Extensions;

namespace Apache.Iggy.Tests.UtilityTests;

public sealed class ToSnakeCaseMessagePolicyTests
{
    [Theory]
    [ClassData(typeof(MyTestDataClass))]
    public void PascalCaseFieldsWillBecomeSnakeCase(string input, string expected)
    {
        var actual = input.ToSnakeCase();
        Assert.Equal(expected, actual);
    }
}
public class MyTestDataClass : IEnumerable<object[]>
{
    public IEnumerator<object[]> GetEnumerator()
    {
        yield return new object[] { "PartitionId", "partition_id" };
        yield return new object[] { "AnotherStringTest", "another_string_test" };
        yield return new object[] { "Id", "id" };
        yield return new object[] { "name", "name" };
        yield return new object[] { "NameTest", "name_test" };
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}