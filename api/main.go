package main

import (
	"log"
	"strconv"
	"time"

	"github.com/gocql/gocql"
)

func main() {
	defer session.Close()

	if err := session.Query(`INSERT INTO events 
		(application, email_id, event, timestamp, email, ip, smtpid, useragent, uuid) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		event.Application,
		event.EmailId,
		event.Event,
		event.Timestamp,
		event.Email,
		event.Ip,
		event.SmtpId,
		event.UserAgent,
		event.UUID).Exec(); err != nil {
		log.Fatal(err)
	}
}

type Event struct {
	Application string
	EmailId     int
	Event       string
	Timestamp   time.Time
	Email       string
	Ip          string
	SmtpId      string
	UserAgent   string
	UUID        gocql.UUID
}

func GetEvent(record []string) (Event, error) {
	timestamp, err := time.Parse("2006-01-02 15:04:05", record[10])
	if err != nil {
		return Event{}, err
	}
	email_id, err := strconv.Atoi(record[9])
	if err != nil {
		email_id = 0
	}
	uuid, err := gocql.RandomUUID()
	if err != nil {
		return Event{}, err
	}

	return Event{
		Application: record[6],
		EmailId:     email_id,
		Event:       record[12],
		Timestamp:   timestamp,
		Email:       record[8],
		Ip:          "",
		SmtpId:      record[11],
		UserAgent:   "",
		UUID:        uuid,
	}, nil
}
