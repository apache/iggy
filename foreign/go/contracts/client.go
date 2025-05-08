package iggcon

type ClientResponse struct {
	ID                  uint32              `json:"id"`
	Address             string              `json:"address"`
	UserID              uint32              `json:"userId"`
	Transport           string              `json:"transport"`
	ConsumerGroupsCount uint32              `json:"consumerGroupsCount"`
	ConsumerGroups      []ConsumerGroupInfo `json:"consumerGroups,omitempty"`
}
