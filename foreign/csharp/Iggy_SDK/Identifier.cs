using Iggy_SDK.Enums;
using System.Buffers.Binary;
using System.Text;

namespace Iggy_SDK;

public readonly struct Identifier : IEquatable<Identifier>
{

    public required IdKind Kind { get; init; }
    public required int Length { get; init; }
    public required byte[] Value { get; init; }

    public static Identifier Numeric(int value)
    {
        byte[] bytes = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(bytes, value);

        return new Identifier
        {
            Kind = IdKind.Numeric,
            Length = 4,
            Value = bytes
        };
    }

    public static Identifier String(string value)
    {
        if (value.Length is 0 or > 255)
        {
            throw new ArgumentException("Value has incorrect size, must be between 1 and 255", nameof(value));
        }
        return new Identifier
        {
            Kind = IdKind.String,
            Length = value.Length,
            Value = Encoding.UTF8.GetBytes(value)
        };
    }

    public override string ToString()
    {
        return Kind switch
        {
            IdKind.Numeric => BitConverter.ToInt32(Value).ToString(),
            IdKind.String => Encoding.UTF8.GetString(Value),
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    public bool Equals(Identifier other)
    {
        return Kind == other.Kind && Value.Equals(other.Value);
    }

    public override bool Equals(object? obj)
    {
        return obj is Identifier other && Equals(other);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine((int)Kind, Value);
    }
}