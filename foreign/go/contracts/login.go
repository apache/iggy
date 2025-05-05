package iggcon

type LogInRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Version  string `json:"version,omitempty"`
	Context  string `json:"context,omitempty"`
}

type LogInAccessTokenRequest struct {
	Token string `json:"token"`
}

type LogInResponse struct {
	UserId uint32 `json:"userId"`
}
