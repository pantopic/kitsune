package main

import (
	"github.com/pantopic/ext-cluster/sdk-go"
	"github.com/pantopic/ext-cluster/sdk-go/pb"
)

func init() {
	cluster.Init(
		cluster.WithComponentReconciler(reconcile),
	)
}

func main() {
}

func reconcile(component *pb.Component) uint32 {
	name := component.Namespace + `.` + component.Resource + `.` + component.Name
	var found bool
	var s *pb.Shard
	for s = range cluster.ShardIter() {
		if s.Name == name+`.queue` {
			found = true
			break
		}
	}
	if !found {
		_, err := cluster.ShardCreate(`queue`, `pantopic/kitsune/storage/queue`,
			cluster.WithTags(component.Tags),
			cluster.Persistent())
		if err != nil {
			println(err)
			return cluster.StatusPending
		}
	}
	return cluster.StatusActive
}
