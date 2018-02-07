package internal

import (
	"encoding/json"
	"time"
)

type GetLogsResponse struct {
	StartTime time.Time  `json:"start_time"`
	EndTime   time.Time  `json:"end_time"`
	NextPage  string     `json:"next_page"`
	Logs      []LogEntry `json:"logs"`
}

const getLogsResponseTimeFormat = time.RFC1123Z

type getLogsResponseInternal struct {
	StartTime string     `json:"start_time"`
	EndTime   string     `json:"end_time"`
	NextPage  string     `json:"next_page"`
	Logs      []LogEntry `json:"logs"`
}

func (g *GetLogsResponse) UnmarshalJSON(b []byte) error {
	glr := &getLogsResponseInternal{}
	err := json.Unmarshal(b, glr)
	if err != nil {
		return err
	}
	g.StartTime, err = time.Parse(getLogsResponseTimeFormat, glr.StartTime)
	if err != nil {
		return err
	}
	g.EndTime, err = time.Parse(getLogsResponseTimeFormat, glr.EndTime)
	if err != nil {
		return err
	}
	g.NextPage = glr.NextPage
	g.Logs = glr.Logs
	return nil
}

func (g *GetLogsResponse) MarshalJSON() ([]byte, error) {
	glr := &getLogsResponseInternal{
		StartTime: g.StartTime.Format(getLogsResponseTimeFormat),
		EndTime:   g.EndTime.Format(getLogsResponseTimeFormat),
		NextPage:  g.NextPage,
		Logs:      g.Logs,
	}
	return json.Marshal(glr)
}

type LogEntry struct {
	IP       string           `json:"ip"`
	Kind     EntryKind        `json:"kind"`
	Time     time.Time        `json:"datetime"`
	Metadata LogEntryMetadata `json:"metadata"`
}

type logEntryInternal struct {
	IP       string           `json:"ip"`
	Kind     EntryKind        `json:"kind"`
	Time     string           `json:"datetime"`
	Metadata LogEntryMetadata `json:"metadata"`
}

func (l *LogEntry) UnmarshalJSON(b []byte) error {
	le := &logEntryInternal{}
	err := json.Unmarshal(b, le)
	if err != nil {
		return err
	}
	l.Time, err = time.Parse(getLogsResponseTimeFormat, le.Time)
	if err != nil {
		return err
	}
	l.IP = le.IP
	l.Kind = le.Kind
	l.Metadata = le.Metadata
	return nil
}

func (l *LogEntry) MarshalJSON() ([]byte, error) {
	le := &logEntryInternal{
		IP:       l.IP,
		Kind:     l.Kind,
		Time:     l.Time.Format(getLogsResponseTimeFormat),
		Metadata: l.Metadata,
	}
	return json.Marshal(le)
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
	Service  json.RawMessage `json:"service"`
	Provider string          `json:"provider"`
}

type EntryKind string

const (
	EntryKindPullRepo = "pull_repo"
)
