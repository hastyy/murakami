package config

import "github.com/hastyy/murakami/internal/server"

type Config struct {
	Server server.Config
}

var DefaultConfig = Config{
	Server: server.DefaultConfig,
}

func (cfg Config) CombineWith(other Config) Config {
	cfg.Server.CombineWith(other.Server)
	return cfg
}
