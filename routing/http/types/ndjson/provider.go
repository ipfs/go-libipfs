package ndjson

import (
	"context"
	"encoding/json"
	"io"

	"github.com/ipfs/go-libipfs/routing/http/types"
	"github.com/ipfs/go-libipfs/routing/http/types/iter"
)

type readProvidersResponseIter struct {
	iter iter.Iter[types.UnknownProviderRecord]
}

func NewReadProvidersResponseIter(ctx context.Context, r io.Reader) *readProvidersResponseIter {
	return &readProvidersResponseIter{iter: iter.FromReaderJSON[types.UnknownProviderRecord](ctx, r)}
}

func (p *readProvidersResponseIter) Next() (types.ProviderResponse, bool, error) {
	v, ok, err := p.iter.Next()
	if !ok {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	switch v.Schema {
	case types.SchemaBitswap:
		var prov types.ReadBitswapProviderRecord
		err := json.Unmarshal(v.Bytes, &prov)
		if err != nil {
			return nil, false, err
		}
		return &prov, true, nil
	default:
		return &v, true, nil
	}
}

func NewWriteProvidersRequestIter(ctx context.Context, r io.Reader) *writeProvidersRequestIter {
	return &writeProvidersRequestIter{iter: iter.FromReaderJSON[types.UnknownProviderRecord](ctx, r)}
}

type writeProvidersRequestIter struct {
	iter iter.Iter[types.UnknownProviderRecord]
}

func (p *writeProvidersRequestIter) Next() (types.WriteProviderRecord, bool, error) {
	v, ok, err := p.iter.Next()
	if !ok {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	switch v.Schema {
	case types.SchemaBitswap:
		var prov types.WriteBitswapProviderRecord
		err := json.Unmarshal(v.Bytes, &prov)
		if err != nil {
			return nil, false, err
		}
		return &prov, true, nil
	default:
		return &v, true, nil
	}
}

func NewWriteProvidersResponseIter(ctx context.Context, r io.Reader) *writeProvidersResponseIter {
	return &writeProvidersResponseIter{iter: iter.FromReaderJSON[types.UnknownProviderRecord](ctx, r)}
}

type writeProvidersResponseIter struct {
	iter iter.Iter[types.UnknownProviderRecord]
}

func (p *writeProvidersResponseIter) Next() (types.ProviderResponse, bool, error) {
	v, ok, err := p.iter.Next()
	if !ok {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	switch v.Schema {
	case types.SchemaBitswap:
		var prov types.WriteBitswapProviderRecordResponse
		err := json.Unmarshal(v.Bytes, &prov)
		if err != nil {
			return nil, false, err
		}
		return &prov, true, nil
	default:
		return &v, true, nil
	}
}
