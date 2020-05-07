package puller

import (
	"context"
	"fmt"
	"testing"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

func TestBigQueryPuller_Pull(t *testing.T) {
	var p BigQueryPuller
	p.GcpProject = "august-gantry-192521"
	p.DatasetName = "ulboralabs"
	ctx := context.Background()
	p.SetContext(ctx)
	client, err := bigquery.NewClient(ctx, p.GcpProject, option.WithCredentialsFile("../gcpCreds.json"))
	if err != nil {
		fmt.Println("bq err: ", err)
	} else {
		p.SetClient(client)
		var query = "SELECT key, lic_name, bus_name, premise_address " +
			"FROM " + p.GcpProject + "." + p.DatasetName + "." + "flic_May_5_2020_18_28_26 " +
			"WHERE premise_zip like @zip "
		var qp []bigquery.QueryParameter
		var par bigquery.QueryParameter
		par.Name = "zip"
		par.Value = "3013%"
		qp = append(qp, par)

		bq := p.GetNew()

		recs := bq.Pull(query, &qp)

		fmt.Println("recs: ", recs)
		if len(*recs) == 0 {
			t.Fail()
		}
	}

}
