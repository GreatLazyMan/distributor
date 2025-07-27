package constant

import "path/filepath"

const (
	DataDir        = "/data"
	NodeNameEnvKey = "NODE_NAME"
	PodIPKey       = "POD_IP"
	WebPort        = "47280"
)

type DownloadFileInfo struct {
	DownloadUrl string
	FileName    string
	ChekSumType string
	ChekSum     string
}

func GetRealName(filename string) string {
	return filepath.Join(DataDir, filename)
}
