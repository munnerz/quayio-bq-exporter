package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/storage"
)

const dataFileName = "export.json"
const dateFormat = "2006-01-02"
const bigQueryDateTimeFormat = "2006-01-02 15:04:05"

type GCSSink struct {
	cl                 *storage.Client
	bucketName, prefix string
	writers            map[string]*storage.Writer
	writerLock         sync.Mutex
}

func NewGCSSink(cl *storage.Client, bucketName, prefix string) *GCSSink {
	return &GCSSink{cl, bucketName, prefix, make(map[string]*storage.Writer), sync.Mutex{}}
}

func (g *GCSSink) Write(entries ...*LogEntry) error {
	entryMap := map[string][]byte{}
	for _, e := range entries {
		fileName := g.buildFileName(e.Time)
		b, err := json.Marshal(e)
		if err != nil {
			return err
		}
		b = append(b, byte('\n'))
		entryMap[fileName] = append(entryMap[fileName], b...)
	}
	for file, entries := range entryMap {
		writer := g.writerForFile(file)
		_, err := writer.Write(entries)
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *GCSSink) writerForFile(s string) *storage.Writer {
	g.writerLock.Lock()
	defer g.writerLock.Unlock()
	w := g.writers[s]
	if w == nil {
		ctx := context.Background()
		w = g.cl.Bucket(g.bucketName).Object(s).NewWriter(ctx)
		g.writers[s] = w
	}
	return w
}

func (g *GCSSink) buildFileName(date time.Time) string {
	s := fmt.Sprintf("%s/%s", date.Format(dateFormat), dataFileName)
	if g.prefix != "" {
		s = fmt.Sprintf("%s/%s", g.prefix, s)
	}
	return s
}

// Exists returns true if there is any existing data for the given date
func (g *GCSSink) Exists(date time.Time) (bool, string, error) {
	filename := g.buildFileName(date)
	ctx := context.Background()
	_, err := g.cl.Bucket(g.bucketName).Object(filename).Attrs(ctx)
	if err == nil {
		return true, "gs://" + g.bucketName + "/" + filename, nil
	}
	if err == storage.ErrObjectNotExist {
		return false, "", nil
	}
	return false, "", err
}

func (g *GCSSink) Close(date time.Time) error {
	g.writerLock.Lock()
	defer g.writerLock.Unlock()
	file := g.buildFileName(date)
	w := g.writers[file]
	if w == nil {
		return nil
	}
	delete(g.writers, file)
	return w.Close()
}
