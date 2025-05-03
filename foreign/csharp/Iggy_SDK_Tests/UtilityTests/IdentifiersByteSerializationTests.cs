using Iggy_SDK;
using Iggy_SDK.Kinds;

namespace Iggy_SDK_Tests.UtilityTests;

public sealed class IdentifiersByteSerializationTests
{

    [Fact]
    public void StringIdentifer_WithInvalidLength_ShouldThrowArgumentException()
    {
        const char character = 'a';
        string val = string.Concat(Enumerable.Range(0, 500).Select(_ => character));

        Assert.Throws<ArgumentException>(() => Identifier.String(val));
    }

    [Fact]
    public void KeyEntityId_WithInvalidLength_ShouldThrowArgumentException()
    {
        const char character = 'a';
        string val = string.Concat(Enumerable.Range(0, 500).Select(_ => character));

        Assert.Throws<ArgumentException>(() => Partitioning.EntityIdString(val));
    }

    [Fact]
    public void KeyBytes_WithInvalidLength_ShouldThrowArgumentException()
    {
        byte[] val = Enumerable.Range(0, 500).Select(x => (byte)x).ToArray();
        Assert.Throws<ArgumentException>(() => Partitioning.EntityIdBytes(val));
    }
}