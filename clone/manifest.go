package clone

type keyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type LocatorManifest struct {
	Index       int    `json:"index"`
	DBType      byte   `json:"db_type"`
	LocatorSize uint32 `json:"locator_size"`
	LocatorB64  string `json:"locator_b64"`
}

type SourceManifest struct {
	Addr string `json:"addr"`
	User string `json:"user"`
}

type OptionsManifest struct {
	Mode          string `json:"mode"`
	Compress      bool   `json:"compress"`
	TLS           bool   `json:"tls"`
	DDLTimeoutSec uint32 `json:"ddl_timeout_sec"`
	BackupLock    bool   `json:"backup_lock"`
	DryRun        bool   `json:"dry_run"`
	ProgressEvery string `json:"progress_every"`
}

type ProtocolManifest struct {
	Requested  uint32 `json:"requested_version"`
	Negotiated uint32 `json:"negotiated_version"`
}

type StatsManifest struct {
	CommandPackets    uint64          `json:"command_packets"`
	ResponsePackets   uint64          `json:"response_packets"`
	DescriptorPackets uint64          `json:"descriptor_packets"`
	DataPackets       uint64          `json:"data_packets"`
	DataBytes         uint64          `json:"data_bytes"`
	DataByLocator     map[byte]uint64 `json:"data_by_locator"`
	RestoredWrites    uint64          `json:"restored_writes"`
	RestoredDataBytes uint64          `json:"restored_data_bytes"`
	RestoredFiles     uint64          `json:"restored_files"`
	ChecksumFiles     uint64          `json:"checksum_files"`
	ChecksumPages     uint64          `json:"checksum_pages"`
}

type Manifest struct {
	ToolVersion string            `json:"tool_version"`
	StartedAt   string            `json:"started_at"`
	FinishedAt  string            `json:"finished_at"`
	Source      SourceManifest    `json:"source"`
	Options     OptionsManifest   `json:"options"`
	Protocol    ProtocolManifest  `json:"protocol"`
	Stats       StatsManifest     `json:"stats"`
	Plugins     []string          `json:"plugins"`
	PluginsV2   []keyValue        `json:"plugins_v2"`
	Configs     map[string]string `json:"configs"`
	ConfigsV3   map[string]string `json:"configs_v3"`
	Collations  []string          `json:"collations"`
	Locators    []LocatorManifest `json:"locators"`
	Warnings    []string          `json:"warnings"`
	Error       string            `json:"error,omitempty"`
}
