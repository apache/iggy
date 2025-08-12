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

using System.Text.Json;
using Apache.Iggy.Contracts.Http;
using Apache.Iggy.Contracts.Http.Auth;
using Apache.Iggy.JsonConfiguration;
using BenchmarkDotNet.Attributes;

namespace Apache.Iggy.Benchmark;

[MemoryDiagnoser]
public class JsonConverterBenchmark
{
    // public static JsonSerializerOptions NewDeserializationOptions { get; } = new()  
    // {        
    //     PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
    // }; 
    //  
    // public static JsonSerializerOptions OldDeserializationOptions { get; } = new()  
    // {        
    //     Converters = { new StatsResponseConverter() }
    // };
    //
    // public string JsonString { get; } =
    //     "{\"process_id\":1,\"cpu_usage\":0.84388185,\"total_cpu_usage\":1.9602673,\"memory_usage\":\"114876 KiB\",\"total_memory\":\"32776332 KiB\",\"available_memory\":\"31216232 KiB\",\"run_time\":5000000,\"start_time\":1754994920000000,\"read_bytes\":\"76 KiB\",\"written_bytes\":\"48 KiB\",\"messages_size_bytes\":\"68 B\",\"streams_count\":1,\"topics_count\":1,\"partitions_count\":2,\"segments_count\":2,\"messages_count\":1,\"clients_count\":11,\"consumer_groups_count\":1,\"hostname\":\"bee7429da00c\",\"os_name\":\"Alpine Linux\",\"os_version\":\"Linux (Alpine Linux 3.22.0)\",\"kernel_version\":\"6.6.87.2-microsoft-standard-WSL2\",\"iggy_server_version\":\"0.5.0\",\"iggy_server_semver\":5000,\"cache_metrics\":{}}";
    //
    //
    // [Benchmark]  
    // public StatsResponse? NewSerialization()  
    // {        
    //     return JsonSerializer.Deserialize<StatsResponse>(JsonString, NewDeserializationOptions);  
    // }  
    //
    // [Benchmark]  
    // public StatsResponse? OldSerialization()  
    // {        
    //     return JsonSerializer.Deserialize<StatsResponse>(JsonString, OldDeserializationOptions);    
    // }
}

//