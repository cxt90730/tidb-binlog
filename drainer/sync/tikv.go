package sync

import (
	"context"
	"github.com/journeymidnight/client-go/config"
	"github.com/journeymidnight/client-go/txnkv"
	"github.com/pingcap/tidb-binlog/drainer/translator"
)

var _ Syncer = &tikvSyncer{}

type tikvSyncer struct {
	*baseSyncer
	cli *TiKVClient
}

type TiKVClient struct {
	TxnCli *txnkv.Client
}

// KV represents a Key-Value pair.
type KV struct {
	K, V []byte
}

func NewClient(pdAddress []string) (*TiKVClient, error) {
	conf := config.Default()
	conf.RPC.MaxConnectionCount = 16
	TxnCli, err := txnkv.NewClient(context.TODO(), pdAddress, conf)
	if err != nil {
		return nil, err
	}
	return &TiKVClient{TxnCli}, nil
}

func NewTikvSyncer(PdAddress []string, tableInfoGetter translator.TableInfoGetter) (*tikvSyncer, error) {
	cli, err := NewClient(PdAddress)
	if err != nil {
		return nil, err
	}
	s := &tikvSyncer{
		baseSyncer: newBaseSyncer(tableInfoGetter),
		cli: cli,
	}
	return s, nil
}

// SetSafeMode should be ignore by tikvSyncer
func (p *tikvSyncer) SetSafeMode(mode bool) bool {
	return false
}

func (p *tikvSyncer) Sync(item *Item) error {
	// Translate
	translator.TiBinlogToTiKV(p.tableInfoGetter, item.Schema, item.Table, item.Binlog, item.PrewriteValue)
	// Sync

	p.success <- item

	return nil
}

func (p *tikvSyncer) Close() error {
	err := p.cli.TxnCli.Close()
	p.setErr(err)
	close(p.success)
	return p.err
}
