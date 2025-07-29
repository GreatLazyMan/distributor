package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/hlog"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/GreatLazyMan/distributor/utils/constant"
)

// 定义字符集
const (
	apikeyLength = 20
	digits       = "0123456789"
	lowerChars   = "abcdefghijklmnopqrstuvwxyz"
	punctChars   = "@^_,."
)

// 生成指定长度的随机字符串
func generateRandomString(length int) (string, error) {
	if length <= 0 {
		return "", fmt.Errorf("长度必须大于 0")
	}

	// 合并所有字符集
	allChars := digits + lowerChars + punctChars

	// 确保至少包含一个数字、一个小写字母和一个标点符号
	result := make([]byte, length)

	// 随机位置放置至少一个数字
	numPos, err := rand.Int(rand.Reader, big.NewInt(int64(length)))
	if err != nil {
		return "", err
	}
	digitPos := int(numPos.Int64())
	digitIdx, err := rand.Int(rand.Reader, big.NewInt(int64(len(digits))))
	if err != nil {
		return "", err
	}
	result[digitPos] = digits[digitIdx.Int64()]

	// 随机位置放置至少一个小写字母
	lowerPos, err := rand.Int(rand.Reader, big.NewInt(int64(length)))
	if err != nil {
		return "", err
	}
	for int(lowerPos.Int64()) == digitPos { // 避免覆盖已放置的数字
		lowerPos, err = rand.Int(rand.Reader, big.NewInt(int64(length)))
		if err != nil {
			return "", err
		}
	}
	lowerIdx, err := rand.Int(rand.Reader, big.NewInt(int64(len(lowerChars))))
	if err != nil {
		return "", err
	}
	result[lowerPos.Int64()] = lowerChars[lowerIdx.Int64()]

	// 随机位置放置至少一个标点符号
	punctPos, err := rand.Int(rand.Reader, big.NewInt(int64(length)))
	if err != nil {
		return "", err
	}
	for int(punctPos.Int64()) == digitPos || punctPos.Int64() == lowerPos.Int64() {
		punctPos, err = rand.Int(rand.Reader, big.NewInt(int64(length)))
		if err != nil {
			return "", err
		}
	}
	punctIdx, err := rand.Int(rand.Reader, big.NewInt(int64(len(punctChars))))
	if err != nil {
		return "", err
	}
	result[punctPos.Int64()] = punctChars[punctIdx.Int64()]

	// 填充剩余位置
	remainingChars := allChars
	remainingLength := len(remainingChars)
	for i := 0; i < length; i++ {
		if i == int(digitPos) || i == int(lowerPos.Int64()) || i == int(punctPos.Int64()) {
			continue // 已填充的位置跳过
		}
		idx, err := rand.Int(rand.Reader, big.NewInt(int64(remainingLength)))
		if err != nil {
			return "", err
		}
		result[i] = remainingChars[idx.Int64()]
	}

	return string(result), nil
}

func main() {
	apiKey, err := generateRandomString(apikeyLength)
	if err != nil {
		hlog.Errorf("generate apikey error: %v", err)
		return
	}
	fmt.Println(apiKey)
	// 创建 Hertz 实例
	h := server.Default(server.WithHostPorts(fmt.Sprintf(":%s", constant.WebPort)))

	downloadDir := constant.DataDir

	// 定义下载路由, 使用通配符参数捕获完整路径
	h.GET("/download/*filepath", func(c context.Context, ctx *app.RequestContext) {

		reqAPIKey := ctx.Request.Header.Peek("X-API-Key")
		if string(reqAPIKey) != apiKey {
			ctx.AbortWithStatus(consts.StatusUnauthorized)
			return
		}

		// 获取文件名参数
		filename := ctx.Param("filepath")
		if filename == "" {
			ctx.AbortWithStatusJSON(consts.StatusBadRequest, map[string]string{
				"error": "Missing filepath",
			})
			return
		}

		// 去除前导斜杠并清理路径
		filename = strings.TrimPrefix(filename, "/")
		filename = filepath.Clean(filename)

		// 构造文件路径
		filePath := filepath.Join(downloadDir, filename)

		// 安全性检查：防止路径遍历攻击
		absDir, err := filepath.Abs(downloadDir)
		if err != nil {
			ctx.AbortWithStatus(consts.StatusInternalServerError)
			return
		}
		absFile, err := filepath.Abs(filePath)
		if err != nil {
			ctx.AbortWithStatus(consts.StatusInternalServerError)
			return
		}

		// 检查路径是否在 downloadDir 下
		rel, err := filepath.Rel(absDir, absFile)
		if err != nil || strings.Contains(rel, "..") {
			ctx.AbortWithStatus(consts.StatusBadRequest)
			return
		}

		if !strings.HasPrefix(absFile, absDir+string(filepath.Separator)) {
			ctx.AbortWithStatus(consts.StatusBadRequest)
			return
		}

		// 检查文件是否存在
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			ctx.AbortWithStatus(consts.StatusNotFound)
			return
		}

		clientIP := ctx.ClientIP()
		hlog.Infof("Download request from %s for file %s", clientIP, filePath)

		// 设置响应头，提示浏览器下载文件
		ctx.Response.Header.Set("Content-Disposition", "attachment")
		ctx.Response.Header.Set("Content-Type", "application/octet-stream")

		// 发送文件
		ctx.File(filePath)
	})

	// 启动服务器
	hlog.Infof("Starting server on :%s...", constant.WebPort)
	h.Spin()
}
