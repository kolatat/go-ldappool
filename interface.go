package ldappool

import (
	"context"

	"github.com/go-ldap/ldap/v3"
)

func (dir *Directory) Search(ctx context.Context, searchRequest *ldap.SearchRequest) (res *ldap.SearchResult, err error) {
	err = dir.retry(func(strategy connReuseStrategy) error {
		res, err = dir.search(ctx, searchRequest, strategy)
		return err
	})
	return res, err
}

func (dir *Directory) search(ctx context.Context, req *ldap.SearchRequest, strategy connReuseStrategy) (*ldap.SearchResult, error) {
	dc, err := dir.conn(ctx, strategy)
	if err != nil {
		return nil, err
	}
	defer dc.releaseConn(err)
	dc.Lock()
	res, err := dc.internal.Search(req)
	dc.Unlock()
	return res, err
}

func (dir *Directory) SearchWithPaging(ctx context.Context, searchRequest *ldap.SearchRequest, pagingSize uint32) (res *ldap.SearchResult, err error) {
	err = dir.retry(func(strategy connReuseStrategy) error {
		res, err = dir.searchWithPaging(ctx, searchRequest, pagingSize, strategy)
		return err
	})
	return res, err
}

func (dir *Directory) searchWithPaging(ctx context.Context, req *ldap.SearchRequest, size uint32, strategy connReuseStrategy) (*ldap.SearchResult, error) {
	dc, err := dir.conn(ctx, strategy)
	if err != nil {
		return nil, err
	}
	defer dc.releaseConn(err)
	dc.Lock()
	res, err := dc.internal.SearchWithPaging(req, size)
	dc.Unlock()
	return res, err
}
