package main

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gocql/gocql"
)

func main() {
	cluster := gocql.NewCluster("172.17.0.2", "172.17.0.3", "172.17.0.4")
	cluster.Keyspace = "testdb"
	cluster.Consistency = gocql.Quorum
	session, _ := cluster.CreateSession()
	defer session.Close()

	if err := createTables(session, false); err != nil {
		log.Fatal("Error creating tables", err.Error())
	}

	//open postgres dump
	file, err := os.Open("/home/rstrong/Downloads/emailtracking-archive-2012.db")
	if err != nil {
		log.Fatal(err)
	}
	tabReader := csv.NewReader(file)
	tabReader.Comma = '\t'
	tabReader.LazyQuotes = true

	events := make(chan Event, 64)

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			for event := range events {
				if err := event.Save(session); err != nil {
					log.Fatal("Could not insert record", err)
				}
			}
			wg.Done()
		}()
	}

	//load up the events
	for {
		record, err := tabReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal("Error reading row", err)
		}

		event, err := GetEventFromSlice(record)
		if err != nil {
			log.Fatal("Could not parse time", err)
		}

		events <- event
	}
}

type Event struct {
	Application string
	EmailId     int
	Event       string
	TimeUUID    gocql.UUID
	Email       string
	Ip          string
	SmtpId      string
	UserAgent   string
}

func GetEventFromSlice(record []string) (Event, error) {
	timestamp, err := time.Parse("2006-01-02 15:04:05", record[10])
	timeUUID := gocql.UUIDFromTime(timestamp)
	if err != nil {
		return Event{}, err
	}
	email_id, err := strconv.Atoi(record[9])
	if err != nil {
		email_id = 0
	}

	return Event{
		Application: record[6],
		EmailId:     email_id,
		Event:       record[12],
		TimeUUID:    timeUUID,
		Email:       record[8],
		Ip:          "",
		SmtpId:      record[11],
		UserAgent:   "",
	}, nil
}

func (e Event) Save(s *gocql.Session) error {
	//insert into events table
	err := s.Query(`INSERT INTO events 
		(application, email_id, event, timeid, email, ip, smtpid, useragent) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		e.Application,
		e.EmailId,
		e.Event,
		e.TimeUUID,
		e.Email,
		e.Ip,
		e.SmtpId,
		e.UserAgent).Exec()
	if err != nil {
		return err
	}

	//update aggregates table
	err = s.Query(`
		UPDATE 
			event_aggregates
		SET 
			count = count + 1
		WHERE 
			application = ? 
			AND email_id = ?
			AND event = ?`, e.Application, e.EmailId, e.Event).Exec()

	return err
}

func createTables(s *gocql.Session, dropExisting bool) error {
	if dropExisting {
		err := s.Query(`DROP TABLE IF EXISTS events`).Exec()
		if err != nil {
			return err
		}
		err = s.Query(`DROP TABLE IF EXISTS event_aggregates`).Exec()
		if err != nil {
			return err
		}
	}
	//creat events table
	err := s.Query(`CREATE TABLE IF NOT EXISTS
			events 
			(application varchar,
			email_id int,
			event varchar, 
			useragent varchar, 
			ip varchar, 
			email varchar, 
			smtpid varchar, 
			timeid timeuuid, 
			PRIMARY KEY ((application, email_id, event), timeid))`).Exec()
	if err != nil {
		return err
	}

	//create aggregate table
	err = s.Query(`CREATE TABLE IF NOT EXISTS
			event_aggregates
			(application varchar,
			email_id int,
			event varchar, 
			count counter,
			PRIMARY KEY ((application, email_id), event))`).Exec()

	return err
}
