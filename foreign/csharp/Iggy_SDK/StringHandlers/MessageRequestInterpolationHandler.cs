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

using System.Runtime.CompilerServices;
using Apache.Iggy.Enums;

namespace Apache.Iggy.StringHandlers;

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