package localdir

import (
	"crypto/md5"
	"encoding/hex"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	ctrl "sigs.k8s.io/controller-runtime"
)

var (
	fileLog = ctrl.Log.WithName("sqlite")
)

type FileManeger struct {
}

func NewFileManager() *FileManeger {
	return &FileManeger{}
}

func (f *FileManeger) ListFiles(root string) ([]string, error) {
	fileList := make([]string, 0)
	// 使用 WalkDir 遍历目录树
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			fileLog.Error(err, "read path %s error", path)
			return nil // 忽略错误，继续遍历
		}

		// 判断是否为文件（非目录）
		if !d.IsDir() {
			fileLog.Info("file is %s", path)
			fileList = append(fileList, path)
		}

		return nil // 继续遍历
	})

	if err != nil {
		fileLog.Error(err, "walk dir %s error", root)
	}
	return fileList, err
}

// 计算文件的 MD5 值
func (f *FileManeger) CalculateMD5(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	buf := make([]byte, 64*1024) // 64KB 缓冲区，适用于大多数文件

	for {
		n, err := file.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
		hash.Write(buf[:n])
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

// 检查文件 MD5 是否与预期一致
func (f *FileManeger) CheckMD5(filePath string, expectedMD5 string) (bool, error) {
	md5Sum, err := f.CalculateMD5(filePath)
	if err != nil {
		return false, err
	}

	return md5Sum == expectedMD5, nil
}
