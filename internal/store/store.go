// Package store persists engine state (window buckets and last quotes) in an
// embedded bbolt database so the rolling statistics survive restarts.
//
// Writes are write-behind: the engine hands over the buckets that changed in
// the last flush interval and a single goroutine commits them in one fsynced
// transaction. A hard crash loses at most one flush interval of updates; a
// graceful shutdown loses nothing.
package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"time"

	bolt "go.etcd.io/bbolt"

	"pricing-engine/internal/metrics"
	"pricing-engine/internal/window"
)

var (
	candlesBucket = []byte("candles") // candles/<symbol>/<start BE u64> -> bucket
	quotesBucket  = []byte("quotes")  // quotes/<symbol> -> last quote
)

const (
	bucketValueLen = 4*8 + 4
	quoteValueLen  = 8 + 8 + 8 + 1
)

// BucketRecord is one window bucket of one symbol.
type BucketRecord struct {
	Symbol string
	Bucket window.Bucket
}

// QuoteRecord is the last accepted price of a symbol.
type QuoteRecord struct {
	Symbol   string
	Bid, Ask float64
	TimeMs   int64
	Decimals uint8
}

// Batch is one flush worth of changes. Later records win over earlier ones.
type Batch struct {
	Buckets []BucketRecord
	Quotes  []QuoteRecord
}

func (b *Batch) Empty() bool { return len(b.Buckets) == 0 && len(b.Quotes) == 0 }

type Store struct {
	db        *bolt.DB
	retention time.Duration
	log       *slog.Logger
}

// Open opens (or creates) the database at path. Buckets older than retention
// are pruned periodically by Run.
func Open(path string, retention time.Duration, log *slog.Logger) (*Store, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open state db %s: %w", path, err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{candlesBucket, quotesBucket} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db, retention: retention, log: log}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// Write commits a batch in one transaction.
func (s *Store) Write(b Batch) error {
	start := time.Now()
	defer func() { metrics.PersistFlushSeconds.Observe(time.Since(start).Seconds()) }()
	return s.db.Update(func(tx *bolt.Tx) error {
		// bbolt requires keys and values to stay valid until the transaction
		// commits, so every Put gets its own slice of one shared allocation.
		candles := tx.Bucket(candlesBucket)
		arena := make([]byte, len(b.Buckets)*(8+bucketValueLen)+len(b.Quotes)*quoteValueLen)
		next := func(n int) []byte {
			s := arena[:n:n]
			arena = arena[n:]
			return s
		}
		var (
			sym    string
			symBkt *bolt.Bucket
		)
		for _, r := range b.Buckets {
			if r.Symbol != sym || symBkt == nil {
				var err error
				sym = r.Symbol
				if symBkt, err = candles.CreateBucketIfNotExists([]byte(sym)); err != nil {
					return err
				}
			}
			key, val := next(8), next(bucketValueLen)
			binary.BigEndian.PutUint64(key, uint64(r.Bucket.Start))
			encodeBucket(val, &r.Bucket)
			if err := symBkt.Put(key, val); err != nil {
				return err
			}
		}
		quotes := tx.Bucket(quotesBucket)
		for _, q := range b.Quotes {
			qval := next(quoteValueLen)
			binary.LittleEndian.PutUint64(qval[0:], math.Float64bits(q.Bid))
			binary.LittleEndian.PutUint64(qval[8:], math.Float64bits(q.Ask))
			binary.LittleEndian.PutUint64(qval[16:], uint64(q.TimeMs))
			qval[24] = q.Decimals
			if err := quotes.Put([]byte(q.Symbol), qval); err != nil {
				return err
			}
		}
		return nil
	})
}

// Load returns all buckets starting at or after sinceMs, and all last quotes.
func (s *Store) Load(sinceMs int64) (map[string][]window.Bucket, map[string]QuoteRecord, error) {
	buckets := make(map[string][]window.Bucket)
	quotes := make(map[string]QuoteRecord)
	err := s.db.View(func(tx *bolt.Tx) error {
		var seek [8]byte
		binary.BigEndian.PutUint64(seek[:], uint64(max(sinceMs, 0)))
		err := tx.Bucket(candlesBucket).ForEachBucket(func(name []byte) error {
			c := tx.Bucket(candlesBucket).Bucket(name).Cursor()
			var bs []window.Bucket
			for k, v := c.Seek(seek[:]); k != nil; k, v = c.Next() {
				if len(k) != 8 || len(v) != bucketValueLen {
					continue
				}
				bs = append(bs, decodeBucket(int64(binary.BigEndian.Uint64(k)), v))
			}
			if len(bs) > 0 {
				buckets[string(name)] = bs
			}
			return nil
		})
		if err != nil {
			return err
		}
		return tx.Bucket(quotesBucket).ForEach(func(k, v []byte) error {
			if len(v) != quoteValueLen {
				return nil
			}
			quotes[string(k)] = QuoteRecord{
				Symbol:   string(k),
				Bid:      math.Float64frombits(binary.LittleEndian.Uint64(v[0:])),
				Ask:      math.Float64frombits(binary.LittleEndian.Uint64(v[8:])),
				TimeMs:   int64(binary.LittleEndian.Uint64(v[16:])),
				Decimals: v[24],
			}
			return nil
		})
	})
	return buckets, quotes, err
}

// Prune deletes buckets that start before beforeMs.
func (s *Store) Prune(beforeMs int64) (int, error) {
	var limit [8]byte
	binary.BigEndian.PutUint64(limit[:], uint64(max(beforeMs, 0)))
	deleted := 0
	err := s.db.Update(func(tx *bolt.Tx) error {
		candles := tx.Bucket(candlesBucket)
		return candles.ForEachBucket(func(name []byte) error {
			b := candles.Bucket(name)
			var old [][]byte
			c := b.Cursor()
			for k, _ := c.First(); k != nil && bytes.Compare(k, limit[:]) < 0; k, _ = c.Next() {
				old = append(old, append([]byte(nil), k...))
			}
			for _, k := range old {
				if err := b.Delete(k); err != nil {
					return err
				}
			}
			deleted += len(old)
			return nil
		})
	})
	return deleted, err
}

// Run commits batches from in until it is closed, pruning expired buckets
// once a minute. It returns after the final batch has been written.
func (s *Store) Run(in <-chan Batch) {
	prune := time.NewTicker(time.Minute)
	defer prune.Stop()
	for {
		select {
		case b, ok := <-in:
			if !ok {
				return
			}
			if err := s.Write(b); err != nil {
				metrics.PersistErrors.Inc()
				s.log.Error("state flush failed", "err", err)
			}
		case <-prune.C:
			n, err := s.Prune(time.Now().Add(-s.retention).UnixMilli())
			if err != nil {
				s.log.Error("prune failed", "err", err)
			} else if n > 0 {
				s.log.Debug("pruned buckets", "count", n)
			}
		}
	}
}

func encodeBucket(dst []byte, b *window.Bucket) {
	binary.LittleEndian.PutUint64(dst[0:], math.Float64bits(b.Open))
	binary.LittleEndian.PutUint64(dst[8:], math.Float64bits(b.High))
	binary.LittleEndian.PutUint64(dst[16:], math.Float64bits(b.Low))
	binary.LittleEndian.PutUint64(dst[24:], math.Float64bits(b.Close))
	binary.LittleEndian.PutUint32(dst[32:], b.Count)
}

func decodeBucket(start int64, v []byte) window.Bucket {
	return window.Bucket{
		Start: start,
		Open:  math.Float64frombits(binary.LittleEndian.Uint64(v[0:])),
		High:  math.Float64frombits(binary.LittleEndian.Uint64(v[8:])),
		Low:   math.Float64frombits(binary.LittleEndian.Uint64(v[16:])),
		Close: math.Float64frombits(binary.LittleEndian.Uint64(v[24:])),
		Count: binary.LittleEndian.Uint32(v[32:]),
	}
}
