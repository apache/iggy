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

using Apache.Iggy.Contracts.Http.Auth;
using Apache.Iggy.Enums;

namespace Apache.Iggy.IggyClient;

public interface IIggyUsers
{
    public Task<UserResponse?> GetUser(Identifier userId, CancellationToken token = default);
    public Task<IReadOnlyList<UserResponse>> GetUsers(CancellationToken token = default);
    public Task<UserResponse?> CreateUser(string userName, string password, UserStatus status, Permissions? permissions = null, CancellationToken token = default);
    public Task DeleteUser(Identifier userId, CancellationToken token = default);
    public Task UpdateUser(Identifier userId, string? userName = null, UserStatus? status = null, CancellationToken token = default);
    public Task UpdatePermissions(Identifier userId, Permissions? permissions = null, CancellationToken token = default);
    public Task ChangePassword(Identifier userId, string currentPassword, string newPassword, CancellationToken token = default);
    public Task<AuthResponse?> LoginUser(string userName, string password, CancellationToken token = default);
    public Task LogoutUser(CancellationToken token = default);
}