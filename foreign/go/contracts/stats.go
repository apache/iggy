package iggcon

type Stats struct {
	ProcessId           int     `json:"process_id"`
	CpuUsage            float32 `json:"cpu_usage"`
	TotalCpuUsage       float32 `json:"total_cpu_usage"`
	MemoryUsage         uint64  `json:"memory_usage"`
	TotalMemory         uint64  `json:"total_memory"`
	AvailableMemory     uint64  `json:"available_memory"`
	RunTime             uint64  `json:"run_time"`
	StartTime           uint64  `json:"start_time"`
	ReadBytes           uint64  `json:"read_bytes"`
	WrittenBytes        uint64  `json:"written_bytes"`
	MessagesSizeBytes   uint64  `json:"messages_size_bytes"`
	StreamsCount        int     `json:"streams_count"`
	TopicsCount         int     `json:"topics_count"`
	PartitionsCount     int     `json:"partitions_count"`
	SegmentsCount       int     `json:"segments_count"`
	MessagesCount       uint64  `json:"messages_count"`
	ClientsCount        int     `json:"clients_count"`
	ConsumerGroupsCount int     `json:"consumer_groups_count"`
	Hostname            string  `json:"hostname"`
	OsName              string  `json:"os_name"`
	OsVersion           string  `json:"os_version"`
	KernelVersion       string  `json:"kernel_version"`
}
