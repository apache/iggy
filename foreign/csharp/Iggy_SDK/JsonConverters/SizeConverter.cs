// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License")

using System.ComponentModel;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Apache.Iggy.JsonConverters;

internal class SizeConverter : JsonConverter<ulong>
{
    public override ulong Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.String)
        {
            return 0;
        }

        var usage = reader.GetString();
        if (string.IsNullOrEmpty(usage))
        {
            return 0;
        }

        var usageStringSplit = usage.Split(' ');
        if (usageStringSplit.Length != 2)
        {
            throw new ArgumentException($"Error Wrong format when deserializing MemoryUsage: {usage}");
        }

        var (memoryUsageBytesVal, memoryUnit) = (ParseFloat(usageStringSplit[0]), usageStringSplit[1]);
        return ConvertStringBytesToUlong(memoryUnit, memoryUsageBytesVal);
    }

    public override void Write(Utf8JsonWriter writer, ulong value, JsonSerializerOptions options)
    {
        throw new NotImplementedException();
    }

    private static ulong ConvertStringBytesToUlong(string memoryUnit, float memoryUsageBytesVal)
    {
        var memoryUsage = memoryUnit switch
        {
            "B" => memoryUsageBytesVal,
            "KiB" => memoryUsageBytesVal * 1024,
            "KB" => memoryUsageBytesVal * (ulong)1e03,
            "MiB" => memoryUsageBytesVal * 1024 * 1024,
            "MB" => memoryUsageBytesVal * (ulong)1e06,
            "GiB" => memoryUsageBytesVal * 1024 * 1024 * 1024,
            "GB" => memoryUsageBytesVal * (ulong)1e09,
            "TiB" => memoryUsageBytesVal * 1024 * 1024 * 1024 * 1024,
            "TB" => memoryUsageBytesVal * (ulong)1e12,
            _ => throw new InvalidEnumArgumentException(
                $"Error Wrong Unit when deserializing MemoryUsage: {memoryUnit}")
        };
        return (ulong)memoryUsage;
    }

    private static float ParseFloat(string value)
    {
        return float.Parse(value, NumberStyles.AllowExponent | NumberStyles.Number, CultureInfo.InvariantCulture);
    }
}