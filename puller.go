package puller

import (
	"context"
	"log"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

//Puller Puller
type Puller interface {
	Pull(query string, params *[]bigquery.QueryParameter) *[][]bigquery.Value
	SetClient(clt *bigquery.Client)
	SetContext(ctx context.Context)
}

//BigQueryPuller BigQueryPuller
type BigQueryPuller struct {
	Ctx         context.Context
	Client      *bigquery.Client
	GcpProject  string
	DatasetName string
}

//GetNew GetNew
func (p *BigQueryPuller) GetNew() Puller {
	return p
}

//Pull Pull
func (p *BigQueryPuller) Pull(query string, params *[]bigquery.QueryParameter) *[][]bigquery.Value {
	var rtn [][]bigquery.Value
	q := p.Client.Query(query)
	q.Parameters = *params
	it, rderr := q.Read(p.Ctx)
	if rderr == nil {
		for {
			var row []bigquery.Value
			err := it.Next(&row)
			if err == iterator.Done {
				break
			}
			if err == nil {
				rtn = append(rtn, row)
			}
		}
	} else {
		log.Println("Big Query Read Err", rderr)
	}
	return &rtn
}

//SetClient SetClient
func (p *BigQueryPuller) SetClient(clt *bigquery.Client) {
	p.Client = clt
}

//SetContext SetContext
func (p *BigQueryPuller) SetContext(ctx context.Context) {
	p.Ctx = ctx
}

//go mod init github.com/Ulbora/BigQueryPuller
