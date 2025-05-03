using Iggy_SDK_Tests.Utils.Messages;

namespace Iggy_SDK_Tests.Utils.DummyObj;

internal static class DummyObjFactory
{
    internal static DummyObject CreateDummyObject()
    {
        return new DummyObject
        {
            Id = Random.Shared.Next(1, 10),
            Text = "TROLOLOLO" + Random.Shared.Next(1, 69)
        };
    }
}