package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/bigquery"
	googleapi "google.golang.org/api/googleapi"

	"github.com/munnerz/quayio-bq-exporter/internal"
)

const (
	dateParamFormat = "01/02/2006"
	repoLogsFormat  = "/api/v1/repository/%s/%s/logs"
)

var (
	authToken             string
	startDate, endDate    string
	namespace, repository string

	projectID, dataset, table string
	create                    bool
)

func init() {
	flag.StringVar(&authToken, "auth-token", "", "quay.io api bearer token to use for requests")
	flag.StringVar(&startDate, "start", time.Now().Format(dateParamFormat), "start date to gather logs from (e.g. "+dateParamFormat+")")
	flag.StringVar(&endDate, "end", time.Now().Format(dateParamFormat), "end date to gather logs to (e.g. "+dateParamFormat+")")
	flag.StringVar(&namespace, "namespace", "", "quay.io repository namespace")
	flag.StringVar(&repository, "repo", "", "quay.io repository name")
	flag.StringVar(&projectID, "project-id", "", "the project ID for the bigquery dataset")
	flag.StringVar(&dataset, "dataset", "", "the bigquery dataset name to write to")
	flag.StringVar(&table, "table", "", "the bigquery table id to write to")
	flag.BoolVar(&create, "create", true, "create bigquery table if it does not exist")
}

func main() {
	flag.Parse()
	if repository == "" {
		log.Fatalf("-repo must be specified")
	}
	if namespace == "" {
		log.Fatalf("-namespace must be specified")
	}
	if authToken == "" {
		log.Fatalf("-auth-token must be specified")
	}
	if projectID == "" {
		log.Fatalf("-project-id must be specified")
	}
	if dataset == "" {
		log.Fatalf("-dataset must be specified")
	}
	if table == "" {
		log.Fatalf("-table must be specified")
	}
	start, err := time.Parse(dateParamFormat, startDate)
	if err != nil {
		log.Fatalf("error parsing start date: %v", err)
	}
	end, err := time.Parse(dateParamFormat, endDate)
	if err != nil {
		log.Fatalf("error parsing end date: %v", err)
	}

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("error creating bigquery client: %v", err)
	}

	table := client.Dataset(dataset).Table(table)
	_, err = table.Metadata(ctx)
	if err != nil {
		err, ok := err.(*googleapi.Error)
		if !ok || !create {
			log.Fatalf("unexpected error: %v", err)
		}
		if err.Code == http.StatusNotFound {
			schema, err := bigquery.InferSchema(&internal.LogEntry{})
			if err != nil {
				log.Fatalf("error inferring bigquery schema: %v", err)
			}
			err = table.Create(ctx, &bigquery.TableMetadata{
				Schema:         schema,
				UseStandardSQL: true,
				UseLegacySQL:   false,
			})
			if err != nil {
				log.Fatalf("error creating table: %v", err)
			}
		}
	}

	grabber := internal.NewLogsGrabber(http.DefaultClient, namespace, repository, authToken)
	logsChan := grabber.ForDateRange(start, end)
	// TODO: buffer results before writing to sink
	for l := range logsChan {
		err := table.Uploader().Put(ctx, l)
		if err != nil {
			multiError, ok := err.(bigquery.PutMultiError)
			if ok {
				for _, err := range multiError {
					log.Printf("Insert error: %v", err.Error())
				}
			}
			log.Fatalf("error writing logs to bigquery: %v", err)
		}
	}
}
