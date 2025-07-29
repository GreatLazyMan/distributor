package constant

import (
	"os"
	"path/filepath"
)

const (
	DataDir        = "/data"
	NodeNameEnvKey = "NODE_NAME"
	PodIPKey       = "POD_IP"
	// TODO: 端口应该作为可修改的参数
	WebPort = "47280"
)

var (
	PodIP    = ""
	NodeName = ""
)

func init() {
	PodIP = os.Getenv(PodIPKey)
	NodeName = os.Getenv(NodeNameEnvKey)
}

type DownloadFileInfo struct {
	DownloadUrl string
	FileName    string
	ChekSumType string
	ChekSum     string
}

func GetRealName(filename string) string {
	return filepath.Join(DataDir, filename)
}
