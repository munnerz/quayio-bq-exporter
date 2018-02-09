package internal

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"
)

const (
	// Date format for requests to the quay.io API
	DateParamFormat = "01/02/2006"

	repoLogsFormat = "/api/v1/repository/%s/%s/logs"
)

type LogsGrabber struct {
	cl                  *http.Client
	ns, repo, authToken string
	alreadyExists       alreadyExistsFunc
}

func defaultFalseAlreadyExistsFunc(time.Time) (bool, string, error) {
	return false, "", nil
}

func NewLogsGrabber(cl *http.Client, ns, repo, authToken string) *LogsGrabber {
	return &LogsGrabber{cl, ns, repo, authToken, defaultFalseAlreadyExistsFunc}
}

func (l *LogsGrabber) SetAlreadyExistsFunc(s alreadyExistsFunc) {
	l.alreadyExists = s
}

func (l *LogsGrabber) ForDateRange(start, end time.Time) map[time.Time]chan *LogEntry {
	return l.performPerDayRequest(start, end)
}

func (l *LogsGrabber) performPerDayRequest(start, end time.Time) map[time.Time]chan *LogEntry {
	out := map[time.Time]chan *LogEntry{}
	dates := []time.Time{}
	for i := 0; ; i++ {
		if end.Before(start) {
			break
		}
		d := start
		start = start.Add(time.Hour * 24)
		exists, _, err := l.alreadyExists(d)
		if err != nil {
			log.Printf("error checking if data already exists for date %q: %v", d.Format(dateFormat), err)
			continue
		}
		if exists {
			log.Printf("skipping day %q as it already exists in storage sink", d.Format(dateFormat))
			continue
		}
		dates = append(dates, d)
		out[d] = make(chan *LogEntry)
	}
	go func() {
		for _, d := range dates {
			func() {
				out := out[d]
				defer close(out)
				// get all logs for the single day 'start'
				respCh := l.getAllLogsForRange(d, d)
				for r := range respCh {
					out <- r
				}
			}()
		}
	}()
	return out
}

// getAllLogsForRange gets all logs in the given range by paging through
// responses.
func (l *LogsGrabber) getAllLogsForRange(start, end time.Time) <-chan *LogEntry {
	out := make(chan *LogEntry)
	go func() {
		defer close(out)
		logs, err := l.getLogs(l.cl, "", start, end)
		if err != nil {
			log.Fatalf("error getting logs: %v", err)
			return
		}
		writeLogEntries(out, logs.Logs...)
		log.Printf("Geting logs between %v and %v", logs.StartTime, logs.EndTime)
		for {
			if logs.NextPage == "" {
				break
			}

			nextLogs, err := l.getLogs(l.cl, logs.NextPage, start, end)
			if err != nil {
				log.Fatalf("error getting logs: %v", err)
				return
			}
			writeLogEntries(out, logs.Logs...)
			logs.NextPage = nextLogs.NextPage
		}
	}()
	return out
}

func writeLogEntries(c chan<- *LogEntry, entries ...LogEntry) {
	for _, e := range entries {
		c <- &e
	}
}

func (l *LogsGrabber) buildRequest(page string, start, end time.Time) (*http.Request, error) {
	values := make(url.Values)
	values["starttime"] = []string{start.Format(DateParamFormat)}
	values["endtime"] = []string{end.Format(DateParamFormat)}
	if page != "" {
		values["next_page"] = []string{page}
	}
	reqURL := url.URL{
		Scheme:   "https",
		Host:     "quay.io",
		Path:     fmt.Sprintf(repoLogsFormat, l.ns, l.repo),
		RawQuery: values.Encode(),
	}
	log.Printf("Building request URL %q", reqURL.String())
	req, err := http.NewRequest("GET", reqURL.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", l.authToken))

	return req, nil
}

// getLogs gets a single page of logs
func (l *LogsGrabber) getLogs(cl *http.Client, page string, start, end time.Time) (*GetLogsResponse, error) {
	req, err := l.buildRequest(page, start, end)
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
		logs := &GetLogsResponse{}

		decoder := json.NewDecoder(resp.Body)
		err = decoder.Decode(logs)
		if err != nil {
			return nil, err
		}
		return logs, nil
	}
}
