package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"

	"cloud.google.com/go/bigquery"

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
}

func buildRequest(nextPage string) (*http.Request, error) {
	values := make(url.Values)
	values["starttime"] = []string{startDate}
	values["endtime"] = []string{endDate}
	if nextPage != "" {
		values["next_page"] = []string{nextPage}
	}
	reqURL := url.URL{
		Scheme:   "https",
		Host:     "quay.io",
		Path:     fmt.Sprintf(repoLogsFormat, namespace, repository),
		RawQuery: values.Encode(),
	}
	log.Printf("Building request URL %q", reqURL.String())
	req, err := http.NewRequest("GET", reqURL.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", authToken))

	return req, nil
}

func getLogs(cl *http.Client, nextPage string) (*internal.GetLogsResponse, error) {
	req, err := buildRequest(nextPage)
	if err != nil {
		return nil, err
	}
	failures := 0
	for {
		resp, err := cl.Do(req)
		if err != nil {
			failures++
			if failures == 3 {
				return nil, err
			}
			log.Printf("HTTP request to quay.io failed, will retry: %v", err)
			time.Sleep(time.Second * 5)
			continue
		}
		// TODO: should this be run irrespective of err != nil?
		defer resp.Body.Close()
		logs := &internal.GetLogsResponse{}

		decoder := json.NewDecoder(resp.Body)
		err = decoder.Decode(logs)
		if err != nil {
			return nil, err
		}
		return logs, nil
	}
}

func getAllLogs() <-chan []internal.LogEntry {
	out := make(chan []internal.LogEntry)
	go func() {
		defer close(out)
		cl := http.DefaultClient
		logs, err := getLogs(cl, "")
		if err != nil {
			log.Fatalf("error getting logs: %v", err)
			return
		}
		log.Printf("Geting logs between %v and %v", logs.StartTime, logs.EndTime)
		out <- logs.Logs
		for {
			if logs.NextPage == "" {
				break
			}

			nextLogs, err := getLogs(cl, logs.NextPage)
			if err != nil {
				log.Fatalf("error getting logs: %v", err)
				return
			}
			out <- logs.Logs
			logs.NextPage = nextLogs.NextPage
		}
	}()
	return out
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

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("error creating bigquery client: %v", err)
	}

	uploader := client.Dataset(dataset).Table(table).Uploader()
	logsChan := getAllLogs()
	for logs := range logsChan {
		log.Printf("Got %d logs", len(logs))
		err := uploader.Put(ctx, logs)
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
