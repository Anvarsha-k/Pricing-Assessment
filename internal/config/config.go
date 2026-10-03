// Package config loads service configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// DefaultSymbols is the subscription list from the assignment.
var DefaultSymbols = []string{
	"AUDUSD", "AUDCAD", "AUDCHF", "AUDJPY", "AUDNZD",
	"AUDSGD", "CADCHF", "CADJPY", "CHFJPY", "CHFSGD",
	"EURAUD", "EURCAD", "EURCHF", "EURGBP", "EURJPY",
	"EURNZD", "EURUSD", "GBPAUD", "GBPCAD", "GBPCHF",
	"GBPJPY", "GBPNZD", "GBPUSD", "NZDCAD", "NZDCHF",
	"NZDJPY", "NZDUSD", "USDCAD", "USDCHF", "USDCNH",
	"USDJPY", "USDMXN", "USDNOK", "USDPLN", "USDSEK",
	"USDSGD", "USDTRY", "USDZAR", "EURTRY", "GBPTRY",
	"NOKJPY", "SEKJPY", "SGDJPY", "ZARJPY", "XAUUSD",
	"XAGUSD", "BTCUSD", "ETHUSD", "BNBUSD", "SOLUSD",
	"XRPUSD", "ADAUSD", "DOGUSD", "DOTUSD", "LTCUSD",
	"BCHUSD", "XLMUSD", "TRXUSD", "UNIUSD", "FILUSD",
	"AVXUSD",
}

type Config struct {
	FeedURL      string
	APIKey       string
	SecretKey    string
	Symbols      []string
	FeedIdle     time.Duration
	BackoffMin   time.Duration
	BackoffMax   time.Duration
	MaxFeedBytes int64

	ListenAddr string
	DataDir    string
	LogLevel   string

	Window         time.Duration
	Resolution     time.Duration
	PriceBasis     string
	SpikePct       float64
	SpikeConfirm   int
	SpikeHold      time.Duration
	MaxClockSkew   time.Duration
	FlushInterval  time.Duration
	Retention      time.Duration
	EngineQueue    int
	MaxFrameBytes  int
	FanoutInterval time.Duration
	MaxClients     int
	MaxConnsPerIP  int
	ControlRate    float64
	ControlBurst   int
	ClientWriteTTL time.Duration
}

// Load reads the configuration from the environment.
func Load() (Config, error) {
	var p parser
	c := Config{
		FeedURL:      p.str("FEED_URL", "wss://v3-prod.livefxhub.com/ws/algo"),
		APIKey:       p.str("API_KEY", ""),
		SecretKey:    p.str("SECRET_KEY", ""),
		Symbols:      p.list("SYMBOLS", DefaultSymbols),
		FeedIdle:     p.dur("FEED_IDLE_TIMEOUT", 30*time.Second),
		BackoffMin:   p.dur("FEED_BACKOFF_MIN", 500*time.Millisecond),
		BackoffMax:   p.dur("FEED_BACKOFF_MAX", 30*time.Second),
		MaxFeedBytes: int64(p.int("FEED_MAX_MESSAGE_BYTES", 4<<20)),

		ListenAddr: p.str("LISTEN_ADDR", ":8080"),
		DataDir:    p.str("DATA_DIR", "./data"),
		LogLevel:   p.str("LOG_LEVEL", "info"),

		Window:         p.dur("WINDOW", 24*time.Hour),
		Resolution:     p.dur("BUCKET", time.Minute),
		PriceBasis:     p.str("PRICE_BASIS", "mid"),
		SpikePct:       p.float("SPIKE_THRESHOLD_PCT", 10),
		SpikeConfirm:   p.int("SPIKE_CONFIRM_TICKS", 3),
		SpikeHold:      p.dur("SPIKE_CONFIRM_AFTER", 10*time.Second),
		MaxClockSkew:   p.dur("MAX_CLOCK_SKEW", 5*time.Second),
		FlushInterval:  p.dur("FLUSH_INTERVAL", time.Second),
		Retention:      p.dur("RETENTION", 48*time.Hour),
		EngineQueue:    p.int("ENGINE_QUEUE", 65536),
		MaxFrameBytes:  p.int("MAX_FRAME_BYTES", 8192),
		FanoutInterval: p.dur("FANOUT_INTERVAL", 0),
		MaxClients:     p.int("MAX_CLIENTS", 10000),
		MaxConnsPerIP:  p.int("MAX_CONNS_PER_IP", 100),
		ControlRate:    p.float("CONTROL_RATE", 10),
		ControlBurst:   p.int("CONTROL_BURST", 20),
		ClientWriteTTL: p.dur("CLIENT_WRITE_TIMEOUT", 5*time.Second),
	}
	if p.err != nil {
		return c, p.err
	}
	return c, c.validate()
}

func (c *Config) validate() error {
	var errs []error
	if c.APIKey == "" || c.SecretKey == "" {
		errs = append(errs, errors.New("API_KEY and SECRET_KEY are required"))
	}
	if c.Resolution <= 0 || c.Window < c.Resolution || c.Window%c.Resolution != 0 {
		errs = append(errs, errors.New("WINDOW must be a positive multiple of BUCKET"))
	}
	if c.Retention < c.Window {
		errs = append(errs, errors.New("RETENTION must be at least WINDOW"))
	}
	if c.MaxFrameBytes < 512 || c.MaxFrameBytes > 10*1024 {
		errs = append(errs, errors.New("MAX_FRAME_BYTES must be within 512..10240"))
	}
	if c.FlushInterval <= 0 || c.EngineQueue <= 0 || c.SpikeConfirm < 1 {
		errs = append(errs, errors.New("FLUSH_INTERVAL, ENGINE_QUEUE and SPIKE_CONFIRM_TICKS must be positive"))
	}
	return errors.Join(errs...)
}

// parser accumulates the first parse error so Load can report it once.
type parser struct{ err error }

func (p *parser) str(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func (p *parser) list(key string, def []string) []string {
	v := p.str(key, "")
	if v == "" {
		return def
	}
	var out []string
	for _, s := range strings.Split(v, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func (p *parser) dur(key string, def time.Duration) time.Duration {
	v := p.str(key, "")
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil && p.err == nil {
		p.err = fmt.Errorf("%s: %w", key, err)
	}
	return d
}

func (p *parser) int(key string, def int) int {
	v := p.str(key, "")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil && p.err == nil {
		p.err = fmt.Errorf("%s: %w", key, err)
	}
	return n
}

func (p *parser) float(key string, def float64) float64 {
	v := p.str(key, "")
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil && p.err == nil {
		p.err = fmt.Errorf("%s: %w", key, err)
	}
	return f
}
