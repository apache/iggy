namespace Iggy_SDK.Utils;

internal static class CommandCodes
{
    internal const int PING_CODE = 1;
    internal const int GET_STATS_CODE = 10;
    internal const int GET_ME_CODE = 20;
    internal const int GET_CLIENT_CODE = 21;
    internal const int GET_CLIENTS_CODE = 22;
    internal const int GET_USER_CODE = 31;
    internal const int GET_USERS_CODE = 32;
    internal const int CREATE_USER_CODE = 33;
    internal const int DELETE_USER_CODE = 34;
    internal const int UPDATE_USER_CODE = 35;
    internal const int UPDATE_PERMISSIONS_CODE = 36;
    internal const int CHANGE_PASSWORD_CODE = 37;
    internal const int LOGIN_USER_CODE = 38;
    internal const int LOGOUT_USER_CODE = 39;
    internal const int GET_PERSONAL_ACCESS_TOKENS_CODE = 41;
    internal const int CREATE_PERSONAL_ACCESS_TOKEN_CODE = 42;
    internal const int DELETE_PERSONAL_ACCESS_TOKEN_CODE = 43;
    internal const int LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE = 44;
    internal const int POLL_MESSAGES_CODE = 100;
    internal const int SEND_MESSAGES_CODE = 101;
    internal const int GET_CONSUMER_OFFSET_CODE = 120;
    internal const int STORE_CONSUMER_OFFSET_CODE = 121;
    internal const int GET_STREAM_CODE = 200;
    internal const int GET_STREAMS_CODE = 201;
    internal const int CREATE_STREAM_CODE = 202;
    internal const int DELETE_STREAM_CODE = 203;
    internal const int UPDATE_STREAM_CODE = 204;
    internal const int GET_TOPIC_CODE = 300;
    internal const int GET_TOPICS_CODE = 301;
    internal const int CREATE_TOPIC_CODE = 302;
    internal const int DELETE_TOPIC_CODE = 303;
    internal const int UPDATE_TOPIC_CODE = 304;
    internal const int CREATE_PARTITIONS_CODE = 402;
    internal const int DELETE_PARTITIONS_CODE = 403;
    internal const int GET_CONSUMER_GROUP_CODE = 600;
    internal const int GET_CONSUMER_GROUPS_CODE = 601;
    internal const int CREATE_CONSUMER_GROUP_CODE = 602;
    internal const int DELETE_CONSUMER_GROUP_CODE = 603;
    internal const int JOIN_CONSUMER_GROUP_CODE = 604;
    internal const int LEAVE_CONSUMER_GROUP_CODE = 605;
}