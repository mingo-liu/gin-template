package config

import (
	"errors"
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr, MySQLDSN, RedisAddr, RedisPassword, MQNameserver, MQTopic, SessionSecret string
	Demo                                                                               bool
	MaxBacklog                                                                         int64
	ReplayAfter                                                                        time.Duration
}

func Load() (Config, error) {
	c := Config{HTTPAddr: env("HTTP_ADDR", ":8080"), MySQLDSN: env("MYSQL_DSN", "likeflow:likeflow@tcp(127.0.0.1:3306)/likeflow?parseTime=true&charset=utf8mb4"), RedisAddr: env("REDIS_ADDR", "127.0.0.1:6379"), RedisPassword: os.Getenv("REDIS_PASSWORD"), MQNameserver: env("MQ_NAMESERVER", "127.0.0.1:9876"), MQTopic: env("MQ_TOPIC", "like-events"), SessionSecret: os.Getenv("SESSION_SECRET"), Demo: os.Getenv("DEMO_MODE") == "true", MaxBacklog: 10000, ReplayAfter: time.Minute}
	if len(c.SessionSecret) < 32 {
		return c, errors.New("SESSION_SECRET must contain at least 32 bytes")
	}
	var err error
	if s := os.Getenv("MAX_BACKLOG"); s != "" {
		c.MaxBacklog, err = strconv.ParseInt(s, 10, 64)
		if err != nil || c.MaxBacklog < 1 {
			return c, errors.New("invalid MAX_BACKLOG")
		}
	}
	return c, nil
}

func env(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}
