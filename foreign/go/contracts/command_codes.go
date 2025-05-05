package iggcon

type CommandCode int

const (
	PingCode                 CommandCode = 1
	GetStatsCode             CommandCode = 10
	GetMeCode                CommandCode = 20
	GetClientCode            CommandCode = 21
	GetClientsCode           CommandCode = 22
	GetUserCode              CommandCode = 31
	GetUsersCode             CommandCode = 32
	CreateUserCode           CommandCode = 33
	DeleteUserCode           CommandCode = 34
	UpdateUserCode           CommandCode = 35
	UpdatePermissionsCode    CommandCode = 36
	ChangePasswordCode       CommandCode = 37
	LoginUserCode            CommandCode = 38
	LogoutUserCode           CommandCode = 39
	GetAccessTokensCode      CommandCode = 41
	CreateAccessTokenCode    CommandCode = 42
	DeleteAccessTokenCode    CommandCode = 43
	LoginWithAccessTokenCode CommandCode = 44
	PollMessagesCode         CommandCode = 100
	SendMessagesCode         CommandCode = 101
	GetOffsetCode            CommandCode = 120
	StoreOffsetCode          CommandCode = 121
	GetStreamCode            CommandCode = 200
	GetStreamsCode           CommandCode = 201
	CreateStreamCode         CommandCode = 202
	DeleteStreamCode         CommandCode = 203
	UpdateStreamCode         CommandCode = 204
	GetTopicCode             CommandCode = 300
	GetTopicsCode            CommandCode = 301
	CreateTopicCode          CommandCode = 302
	DeleteTopicCode          CommandCode = 303
	UpdateTopicCode          CommandCode = 304
	CreatePartitionsCode     CommandCode = 402
	DeletePartitionsCode     CommandCode = 403
	GetGroupCode             CommandCode = 600
	GetGroupsCode            CommandCode = 601
	CreateGroupCode          CommandCode = 602
	DeleteGroupCode          CommandCode = 603
	JoinGroupCode            CommandCode = 604
	LeaveGroupCode           CommandCode = 605
)

//    internal const int GET_PERSONAL_ACCESS_TOKENS_CODE = 41;
//    internal const int CREATE_PERSONAL_ACCESS_TOKEN_CODE = 42;
//    internal const int DELETE_PERSONAL_ACCESS_TOKEN_CODE = 43;
//    internal const int LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE = 44;
