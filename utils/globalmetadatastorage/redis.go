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
