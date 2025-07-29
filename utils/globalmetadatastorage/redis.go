package globalmetadatastorage

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/GreatLazyMan/distributor/utils/constant"
)

var (
	redisLog = ctrl.Log.WithName("redis")
)

const (
	FileNameKey     = "filename"
	CheckSumKey     = "checksum"
	CheckSumTypeKey = "checksumtype"
	StreamPrefix    = "stream:events"
	EOFKey          = "EOF"
	lockExpiration  = 10 * time.Second       // 锁的过期时间，防止死锁
	retryInterval   = 200 * time.Millisecond // 重试间隔
	maxRetries      = 5                      // 最大重试次数
	MaxConnection   = 4
)

var (
	nodeName      = ""
	consumerLimit = 10
)

func init() {
	nodeName = os.Getenv(constant.NodeNameEnvKey)
}

type RedisEngine struct {
	*redis.Client
}

func NewRedisClient(redisHost, redisPort, redisPass string) *RedisEngine {
	redisClient := redis.NewClient(&redis.Options{
		Addr:       fmt.Sprintf("%s:%s", redisHost, redisPass),
		Password:   redisHost,
		MaxRetries: 3,
	})
	return &RedisEngine{redisClient}
}

func (r *RedisEngine) ProduceMessage(streamName string, ms ...map[string]string) error {
	ctx := context.Background()
	for m := range ms {
		_, err := r.XAdd(ctx, &redis.XAddArgs{
			Stream: fmt.Sprintf("%s:%s", StreamPrefix, streamName),
			Values: m,
		}).Result()
		if err != nil {
			redisLog.Error(err, "add redis message  %v error", m)
			return err
		}
	}
	return nil
}

func (r *RedisEngine) ConsumeMessage(streamName string) (chan map[string]interface{}, error) {

	ctx := context.Background()

	groupName := nodeName
	consumerName := fmt.Sprintf("%s-%d", nodeName, os.Getpid()) // 使用进程 ID 作为消费者名称

	StreamName := fmt.Sprintf("%s:%s", StreamPrefix, streamName)
	// 创建消费者组（只需执行一次）
	err := r.XGroupCreateMkStream(ctx, StreamName, groupName, "0").Err()
	if err != nil {
		if err != redis.Nil { // 如果组已存在，redis.Nil 错误是正常的
			redisLog.Error(err, "create consumer group %s error", groupName)
			return nil, err
		}
	}

	consumeChan := make(chan map[string]interface{}, consumerLimit)
	go func() {
		// 启动主消费循环
		ticker := time.NewTicker(5 * time.Second)
		quit := make(chan string, 0)
		defer ticker.Stop()
		defer close(quit)
		defer close(consumeChan)
		go r.consumeMessages(ctx, groupName, consumerName, StreamName, consumeChan, quit)
		// 处理历史遗留的未确认消息
		for {
			select {
			case <-ticker.C:
				r.processPendingMessages(ctx, groupName, consumerName, StreamName, consumeChan)
			case <-quit:
				r.processPendingMessages(ctx, groupName, consumerName, StreamName, consumeChan)
				return
			}
		}
	}()

	return consumeChan, nil
}

func (r *RedisEngine) processPendingMessages(ctx context.Context, groupName, consumerName, streamKey string,
	consumeChan chan map[string]interface{}) {
	// 获取 PEL 中的未确认消息
	pending, err := r.XPending(ctx, streamKey, groupName).Result()
	if err != nil {
		redisLog.Error(err, "get pending message error")
		return
	}

	if pending.Count > 0 {

		// 获取详细的未确认消息列表
		pendingEntries, err := r.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream:   streamKey,
			Group:    groupName,
			Start:    "0",
			End:      "+",
			Count:    100,
			Consumer: consumerName,
		}).Result()

		if err != nil {
			redisLog.Error(err, "get pending message detail error")
			return
		}

		// 处理历史遗留的未确认消息
		for _, entry := range pendingEntries {

			// 重新获取消息内容并处理
			streams, err := r.XRead(ctx, &redis.XReadArgs{
				Streams: []string{streamKey, entry.ID},
				Count:   1,
			}).Result()

			if err != nil {
				redisLog.Error(err, "read histrical message %v  error", entry.ID)
				continue
			}

			// 处理消息内容
			for _, stream := range streams {
				for _, msg := range stream.Messages {

					// 业务处理逻辑...
					consumeChan <- msg.Values

					// 确认消息处理完成
					// TODO: 应该reconsile中处理完成后返回确认是否ACK
					_, err := r.XAck(ctx, streamKey, groupName, msg.ID).Result()
					if err != nil {
						redisLog.Error(err, " ack message error ID=%s: error", msg.ID)
					}
				}
			}
		}
	}
}

func (r *RedisEngine) consumeMessages(ctx context.Context, groupName, consumerName, streamKey string,
	consumeChan chan map[string]interface{}, quit chan string) {
	// 持续消费消息
	for {
		// 使用 ">" 作为 ID，表示只获取未分配给任何消费者的新消息
		streams, err := r.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamKey, ">"},
			Count:    1,
			Block:    5000 * time.Millisecond,
		}).Result()

		if err != nil {
			if err != redis.Nil {
				redisLog.Error(err, "read message error")
			}
			continue
		}

		// 处理接收到的消息
		for _, stream := range streams {
			for _, msg := range stream.Messages {

				// 业务处理逻辑...
				consumeChan <- msg.Values
				// 确认消息处理完成
				_, err := r.XAck(ctx, streamKey, groupName, msg.ID).Result()
				if err != nil {
					redisLog.Error(err, " ack message error ID=%s: error", msg.ID)
				}
				if _, ok := msg.Values[EOFKey]; ok {
					quit <- ""
					return
				}
			}
		}
	}
}

// 获取分布式锁
func (r *RedisEngine) acquireLock(ctx context.Context, lockKey string) (bool, error) {
	// 使用 SETNX 原子操作获取锁，设置过期时间防止死锁
	acquired, err := r.SetNX(ctx, lockKey, "1", lockExpiration).Result()
	return acquired, err
}

// 带超时的阻塞锁
func (r *RedisEngine) acquireLockWithTimeout(ctx context.Context, timeout time.Duration, lockKey string) (bool, error) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		acquired, err := r.SetNX(ctx, lockKey, "1", lockExpiration).Result()
		if err != nil {
			return false, err
		}
		if acquired {
			return true, nil
		}
		time.Sleep(retryInterval)
	}

	return false, nil // 超时返回
}

// 释放分布式锁
func (r *RedisEngine) releaseLock(ctx context.Context, lockKey string) error {
	return r.Del(ctx, lockKey).Err()
}

// 带重试的锁获取
func (r *RedisEngine) acquireLockWithRetry(ctx context.Context, lockKey string) (bool, error) {
	var retries int
	for {
		acquired, err := r.acquireLock(ctx, lockKey)
		if err != nil {
			return false, err
		}
		if acquired {
			return true, nil
		}

		retries++
		if retries > maxRetries {
			return false, fmt.Errorf("get lock timeout, retriy times: %d", retries)
		}

		// 等待一段时间后重试
		time.Sleep(retryInterval)
	}
}

// 带重试机制的 Pipeline 执行函数
func (r *RedisEngine) executePipelineWithRetry(ctx context.Context, fn func(pipe redis.Pipeliner) error, maxRetries int) error {
	var lastErr error

	for i := 0; i <= maxRetries; i++ {
		if i > 0 {
			// 重试前等待一段时间（指数退避）
			backoff := time.Duration(100*i) * time.Millisecond
			time.Sleep(backoff)
			fmt.Printf("Pipeline 执行失败，正在重试 (%d/%d)...\n", i, maxRetries)
		}

		pipe := r.TxPipeline()
		if err := fn(pipe); err != nil {
			lastErr = fmt.Errorf("构建 Pipeline 失败: %w", err)
			continue
		}

		_, err := pipe.Exec(ctx)
		if err == nil {
			return nil // 执行成功
		}

		lastErr = fmt.Errorf("执行 Pipeline 失败: %w", err)
	}

	return fmt.Errorf("重试 %d 次后仍然失败: %w", maxRetries, lastErr)
}

func (r *RedisEngine) updateLeaderboard(ctx context.Context, filePath string) error {
	// 获取锁
	lockkey := fmt.Sprintf("lock:%s", filePath)
	acquired, err := r.acquireLockWithRetry(ctx, lockkey)
	if err != nil {
		return fmt.Errorf("get lock %s error: %v", lockkey, err)
	}
	if !acquired {
		return fmt.Errorf("get lock %s timeout", lockkey)
	}
	defer r.releaseLock(ctx, lockkey) // 确保锁最终被释放

	downloadUrl := fmt.Sprintf("http://%s:%s/download/%s", constant.PodIP, constant.WebPort, filePath)
	// 执行带重试的 Pipeline
	return r.executePipelineWithRetry(ctx, func(pipe redis.Pipeliner) error {
		// 定义 Pipeline 中的操作
		pipe.ZAdd(ctx, filePath, redis.Z{Score: MaxConnection, Member: downloadUrl})
		return nil
	}, 3) // 最多重试 3 次
}
