// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License")

namespace Apache.Iggy.Exceptions;

/// <summary>
///     Exception thrown when a consumer group is not found
/// </summary>
public class ConsumerGroupNotFoundException : Exception
{
    /// <summary>
    ///     Name of the consumer group that was not found
    /// </summary>
    public string ConsumerGroupName { get; }

    /// <summary>
    ///     Initializes a new instance of the <see cref="ConsumerGroupNotFoundException" /> class.
    /// </summary>
    public ConsumerGroupNotFoundException(string consumerGroupName) : base(
        $"Consumer Group {consumerGroupName} not found")
    {
        ConsumerGroupName = consumerGroupName;
    }
}
