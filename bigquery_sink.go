package flow

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/google/uuid"
	"google.golang.org/api/googleapi"
)

type bqSink struct {
	client  *bigquery.Client
	dataset *bigquery.Dataset
}

type row struct {
	Fact
	InsertId string `bigquery:"insertId"` // Idempotent writes.
}

func NewBigquerySink(ctx context.Context, projectID, datasetID string) (*bqSink, error) {
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}
	dataset := client.Dataset(datasetID)

	return &bqSink{client, dataset}, nil
}

func (s *bqSink) Write(ctx context.Context, topic Topic, facts ...Fact) error {
	// TODO(bilus): This is temporary code using streaming inserts. Load jobs should
	// be used eventually.

	table := s.dataset.Table(string(topic))
	ins := table.Inserter()
	rows, err := rows(facts)
	if err != nil {
		return err
	}
	err = ins.Put(ctx, rows)
	if err != nil && hasStatusCode(err, 404) {
		err = s.createTable(ctx, table)
		if err != nil {
			return err
		}
		// After creating the table, there's still a good chance the inserts will fail
		// because the operation is eventually consistent. Just keep retrying.
		return ins.Put(ctx, rows)
	}
	return err
}

func (s *bqSink) Heartbeat(ctx context.Context) {
}

func (s *bqSink) createTable(ctx context.Context, table *bigquery.Table) error {
	schema, err := bigquery.InferSchema(row{})
	if err != nil {
		return err
	}
	return table.Create(ctx, &bigquery.TableMetadata{Schema: schema})
}

func rows(facts []Fact) ([]row, error) {
	rows := make([]row, len(facts))
	for i, fact := range facts {
		rows[i].Fact = fact
		factID := fact.FactID
		if factID == "" {
			uuid, err := uuid.NewRandom()
			if err != nil {
				return nil, err
			}
			factID = uuid.String()
			rows[i].Fact.FactID = factID
		}
		rows[i].InsertId = factID
	}
	return rows, nil
}

func hasStatusCode(err error, code int) bool {
	if e, ok := err.(*googleapi.Error); ok && e.Code == code {
		return true
	}
	return false
}
