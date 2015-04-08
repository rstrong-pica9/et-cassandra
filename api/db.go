package main

import (
	"github.com/gocql/gocql"
)

var keyspace = "testdb"
var clusterAddresses = []string{"localhost"}

func GetDb() (*gocql.Session, error) {
	cluster := gocql.NewCluster(clusterAddresses...)
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.Quorum
	session, err := cluster.CreateSession()
	return session, err
}
