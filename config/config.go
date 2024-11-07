package config

import (
	"os"
)

type Config struct {
	RedisAddress string
	KafkaAddress string
}

func LoadConfig() *Config {
	return &Config{
		RedisAddress: getEnv("REDIS_ADDRESS", "localhost:6379"),
		KafkaAddress: getEnv("KAFKA_ADDRESS", "localhost:9092"),
	}
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

// is there a hint in the case study where there is a way in which the customer is keen on giving
// feedback
//Knowledge based recommender system
//1. Constraint based recommender system
//2. Case based recommender system
// user specify the target/anchor point
// Entry point is user requirements
// Entry point is target output
// Similarity metrics
// Critiquing metrics
// How will Similarity metrics help me get similar kind of data
