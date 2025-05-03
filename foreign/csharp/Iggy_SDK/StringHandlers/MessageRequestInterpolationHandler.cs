using Iggy_SDK.Enums;
using System.Runtime.CompilerServices;

namespace Iggy_SDK.StringHandlers;

[InterpolatedStringHandler]
internal ref struct MessageRequestInterpolationHandler
{
    private DefaultInterpolatedStringHandler _innerHandler;
    internal MessageRequestInterpolationHandler(int literalLength, int formattedCount)
    {
        _innerHandler = new DefaultInterpolatedStringHandler(literalLength, formattedCount);
    }

    internal void AppendLiteral(string message)
    {
        _innerHandler.AppendLiteral(message);
    }
    internal void AppendFormatted<T>(T t)
    {
        switch (t)
        {
            case MessagePolling pollingStrat:
                {
                    var str = pollingStrat switch
                    {
                        MessagePolling.Offset => "offset",
                        MessagePolling.Timestamp => "timestamp",
                        MessagePolling.First => "first",
                        MessagePolling.Last => "last",
                        MessagePolling.Next => "next",
                        _ => throw new ArgumentOutOfRangeException()
                    };
                    _innerHandler.AppendFormatted(str);
                    break;
                }
            case bool tBool:
                _innerHandler.AppendFormatted(tBool.ToString().ToLower());
                break;
            default:
                _innerHandler.AppendFormatted(t);
                break;
        }
    }

    public override string ToString()
    {
        return _innerHandler.ToString();
    }

    public string ToStringAndClear()
    {
        return _innerHandler.ToStringAndClear();
    }
}