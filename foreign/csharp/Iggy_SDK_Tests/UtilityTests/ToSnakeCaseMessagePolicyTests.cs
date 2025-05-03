using Iggy_SDK.Extensions;
using System.Collections;

namespace Iggy_SDK_Tests.UtilityTests;

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