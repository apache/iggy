// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License")

namespace Apache.Iggy.Exceptions;

public class ConsumerGroupNotFoundException : Exception
{
    public string ConsumerGroupName { get; }

    public ConsumerGroupNotFoundException(string consumerGroupName) : base($"Consumer Group {consumerGroupName} not found")
    {
        ConsumerGroupName = consumerGroupName;
    }
}
