package iggcon

import "time"

type CreateAccessTokenRequest struct {
	Name   string `json:"Name"`
	Expiry uint32 `json:"Expiry"`
}

type DeleteAccessTokenRequest struct {
	Name string `json:"Name"`
}

type AccessTokenResponse struct {
	Name   string     `json:"Name"`
	Expiry *time.Time `json:"Expiry"`
}

type AccessToken struct {
	Token string `json:"token"`
}
