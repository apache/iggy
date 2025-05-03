namespace Iggy_SDK.Enums;


//By offset (using the indexes)
//By timestamp (using the time indexes)
//First/Last N messages
//Next N messages for the specific consumer

public enum MessagePolling
{
    Offset,
    Timestamp,
    First,
    Last,
    Next
}