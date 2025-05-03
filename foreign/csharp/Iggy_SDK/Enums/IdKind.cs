
using System.Runtime.CompilerServices;
namespace Iggy_SDK.Enums;

public enum IdKind
{
    Numeric,
    String
}

internal static class Extensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static byte GetByte(this IdKind kind)
    {
        return kind switch
        {
            IdKind.Numeric => 1,
            IdKind.String => 2,
            _ => throw new ArgumentOutOfRangeException()
        };
    }
}