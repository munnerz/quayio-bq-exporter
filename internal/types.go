package internal

import (
	"time"

	"cloud.google.com/go/bigquery"
)

// alreadyExistsFunc returns true if a data file for the given date already
// exists. The returned string is an optional 'reference' to the file.
type alreadyExistsFunc func(time.Time) (bool, string, error)

var LogEntrySchema = bigquery.Schema{
	{Name: "ip", Type: bigquery.StringFieldType},
	{Name: "kind", Type: bigquery.StringFieldType},
	{Name: "datetime", Type: bigquery.DateTimeFieldType},
	{
		Name: "metadata",
		Type: bigquery.RecordFieldType,
		Schema: bigquery.Schema{
			{Name: "repo", Type: bigquery.StringFieldType},
			{Name: "tag", Type: bigquery.StringFieldType},
			{Name: "namespace", Type: bigquery.StringFieldType},
			{Name: "public", Type: bigquery.BooleanFieldType},
			{
				Name: "resolved_ip",
				Type: bigquery.RecordFieldType,
				Schema: bigquery.Schema{
					{Name: "sync_token", Type: bigquery.StringFieldType},
					{Name: "region", Type: bigquery.StringFieldType},
					{Name: "provider", Type: bigquery.StringFieldType},
				},
			},
		},
	},
}

type GetLogsResponse struct {
	StartTime time.Time  `json:"start_time"`
	EndTime   time.Time  `json:"end_time"`
	NextPage  string     `json:"next_page"`
	Logs      []LogEntry `json:"logs"`
}

type LogEntry struct {
	IP       string           `json:"ip"`
	Kind     EntryKind        `json:"kind"`
	Time     time.Time        `json:"datetime"`
	Metadata LogEntryMetadata `json:"metadata"`
}

type LogEntryMetadata struct {
	Repo       string     `json:"repo"`
	Tag        string     `json:"tag"`
	Namespace  string     `json:"namespace"`
	Public     bool       `json:"public"`
	ResolvedIP LogEntryIP `json:"resolved_ip"`
}

type LogEntryIP struct {
	SyncToken string `json:"sync_token"`
	Region    string `json:"region"`
	// todo: not sure what type this field is
	// Service  json.RawMessage `json:"service"`
	Provider string `json:"provider"`
}

type EntryKind string

const (
	EntryKindPullRepo = "pull_repo"
)
