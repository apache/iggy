package iggcon

type ChangePasswordRequest struct {
	UserID          Identifier `json:"-"`
	CurrentPassword string     `json:"CurrentPassword"`
	NewPassword     string     `json:"NewPassword"`
}

type UpdateUserPermissionsRequest struct {
	UserID      Identifier   `json:"-"`
	Permissions *Permissions `json:"Permissions,omitempty"`
}

type UpdateUserRequest struct {
	UserID   Identifier  `json:"-"`
	Username string      `json:"username"`
	Status   *UserStatus `json:"userStatus"`
}

type CreateUserRequest struct {
	Username    string       `json:"username"`
	Password    string       `json:"Password"`
	Status      UserStatus   `json:"Status"`
	Permissions *Permissions `json:"Permissions,omitempty"`
}

type UserResponse struct {
	Id          uint32       `json:"Id"`
	CreatedAt   uint64       `json:"CreatedAt"`
	Status      UserStatus   `json:"Status"`
	Username    string       `json:"Username"`
	Permissions *Permissions `json:"Permissions"`
}

type UserStatus int

const (
	Active UserStatus = iota
	Inactive
)

type Permissions struct {
	Global  GlobalPermissions          `json:"Global"`
	Streams map[int]*StreamPermissions `json:"Streams,omitempty"`
}

type GlobalPermissions struct {
	ManageServers bool `json:"ManageServers"`
	ReadServers   bool `json:"ReadServers"`
	ManageUsers   bool `json:"ManageUsers"`
	ReadUsers     bool `json:"ReadUsers"`
	ManageStreams bool `json:"ManageStreams"`
	ReadStreams   bool `json:"ReadStreams"`
	ManageTopics  bool `json:"ManageTopics"`
	ReadTopics    bool `json:"ReadTopics"`
	PollMessages  bool `json:"PollMessages"`
	SendMessages  bool `json:"SendMessages"`
}

type StreamPermissions struct {
	ManageStream bool                      `json:"ManageStream"`
	ReadStream   bool                      `json:"ReadStream"`
	ManageTopics bool                      `json:"ManageTopics"`
	ReadTopics   bool                      `json:"ReadTopics"`
	PollMessages bool                      `json:"PollMessages"`
	SendMessages bool                      `json:"SendMessages"`
	Topics       map[int]*TopicPermissions `json:"Topics,omitempty"`
}

type TopicPermissions struct {
	ManageTopic  bool `json:"ManageTopic"`
	ReadTopic    bool `json:"ReadTopic"`
	PollMessages bool `json:"PollMessages"`
	SendMessages bool `json:"SendMessages"`
}
