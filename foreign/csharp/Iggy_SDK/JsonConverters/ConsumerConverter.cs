// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License.  You may obtain a copy of the License at
// //
// //   http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing,
// // software distributed under the License is distributed on an
// // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// // KIND, either express or implied.  See the License for the
// // specific language governing permissions and limitations
// // under the License.

using System.Text.Json;
using System.Text.Json.Serialization;
using Apache.Iggy.Enums;
using Apache.Iggy.Kinds;

namespace Apache.Iggy.JsonConverters;

internal sealed class ConsumerConverter : JsonConverter<Consumer>
{
    public override Consumer Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        throw new NotImplementedException();
    }

    public override void Write(Utf8JsonWriter writer, Consumer value, JsonSerializerOptions options)
    {
        switch (value.ConsumerId.Kind)
        {
            case IdKind.Numeric:
                writer.WriteNumberValue(value.ConsumerId.GetUInt32());
                break;
            case IdKind.String:
                writer.WriteStringValue(value.ConsumerId.GetString());
                break;
        }
    }
}
