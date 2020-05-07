package puller

import (
	"context"

	"cloud.google.com/go/bigquery"
)

//MockPuller MockPuller
type MockPuller struct {
	Ctx         context.Context
	Client      *bigquery.Client
	GcpProject  string
	DatasetName string
	MockResp    [][]bigquery.Value
}

//GetNew GetNew
func (p *MockPuller) GetNew() Puller {
	return p
}

//Pull Pull
func (p *MockPuller) Pull(query string, params *[]bigquery.QueryParameter) *[][]bigquery.Value {
	return &p.MockResp
}

//SetClient SetClient
func (p *MockPuller) SetClient(clt *bigquery.Client) {
	p.Client = clt
}

//SetContext SetContext
func (p *MockPuller) SetContext(ctx context.Context) {
	p.Ctx = ctx
}
