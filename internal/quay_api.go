package internal

import (
	"encoding/json"
	"time"
)

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
		StartTime: g.StartTime.Format(bigQueryDateTimeFormat),
		EndTime:   g.EndTime.Format(bigQueryDateTimeFormat),
		NextPage:  g.NextPage,
		Logs:      g.Logs,
	}
	return json.Marshal(glr)
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
		Time:     l.Time.Format(bigQueryDateTimeFormat),
		Metadata: l.Metadata,
	}
	return json.Marshal(le)
}
