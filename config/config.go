package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

type Config struct{ 
	Server Server `yaml:"server"`
	Backends []Resource `yaml:"backends"`
	Fallback Fallback `yaml:"fallback"`
}

type Server struct{ 
	Port int `yaml:"port"`
	ReadTimeoutSeconds int `yaml:"read_timeout_seconds"`
}

type Resource struct{ 
	ID string `yaml:"id"`
	URL string `yaml:"url"`
	MaxTokens int `yaml:"max_tokens_capacity"`
	Models []string `yaml:"models"`
}

type Fallback struct{ 
	Enabled bool `yaml:"enabled"`
	Url string `yaml:"url"`
	ApiKey string `yaml:"api_key"`
}

func NewConfig() (*Config, error) { 
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	err := viper.ReadInConfig()
	if err != nil {
		return nil, fmt.Errorf("Error loading environmental variables from config.yaml: %s", err)
	}
	c := &Config{}
	err = viper.Unmarshal(c)
	if err != nil {
		return nil, fmt.Errorf("Error reading environmental variables from config.yaml: %s", err)
	}	
	return c, nil
}