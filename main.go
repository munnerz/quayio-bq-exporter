package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
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

	bucketName string
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
	flag.StringVar(&bucketName, "bucket", "quayio-log-exports", "the GCS bucket name to store exports in")
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
			err := table.Create(ctx, &bigquery.TableMetadata{
				Schema: internal.LogEntrySchema,
			})
			if err != nil {
				log.Fatalf("error creating table: %v", err)
			}
		}
	}

	storageCl, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("error creating storage client: %v", err)
	}
	grabber := internal.NewLogsGrabber(http.DefaultClient, namespace, repository, authToken)
	sink := internal.NewGCSSink(storageCl, bucketName, fmt.Sprintf("%s/%s", namespace, repository))
	grabber.SetAlreadyExistsFunc(sink.Exists)
	logChans := grabber.ForDateRange(start, end)
	var wg sync.WaitGroup
	wg.Add(len(logChans))
	for date, ch := range logChans {
		go func(date time.Time, ch <-chan *internal.LogEntry) {
			// this will be called twice in the success case, but that's okay
			defer sink.Close(date)
			defer wg.Done()
			err := sinkChan(ch, sink)
			if err != nil {
				log.Fatalf("error writing log entry: %v", err)
			}
			err = sink.Close(date)
			if err != nil {
				log.Fatalf("error saving file to gcs: %v", err)
			}
		}(date, ch)
	}

	wg.Wait()
	var refs []string
	for d, _ := range logChans {
		exists, ref, err := sink.Exists(d)
		if err != nil {
			log.Fatalf("error checking if data file exists: %v", err)
		}
		if !exists {
			log.Printf("could not find data file for date %q", d)
			continue
		}
		log.Printf("Loading data file %q", ref)
		refs = append(refs, ref)
	}

	gcsRef := bigquery.NewGCSReference(refs...)
	gcsRef.AutoDetect = true
	gcsRef.FileConfig = bigquery.FileConfig{
		SourceFormat: bigquery.JSON,
	}
	loader := table.LoaderFrom(gcsRef)
	job, err := loader.Run(ctx)
	if err != nil {
		log.Fatalf("Erroring running import job: %v", err)
	}
	_, err = job.Wait(ctx)
	if err != nil {
		log.Fatalf("Error waiting for import job to complete: %v", err)
	}
}

func sinkChan(ch <-chan *internal.LogEntry, sink *internal.GCSSink) error {
	for e := range ch {
		if err := sink.Write(e); err != nil {
			return err
		}
	}
	return nil
}
