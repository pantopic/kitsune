package main

import (
	"maps"

	"github.com/pantopic/ext-cluster/sdk-go"
	"github.com/pantopic/ext-cluster/sdk-go/pb"
)

const (
	ShardTypeQueue = "pantopic/kitsune/storage/queue"
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
		tags := make(map[string]string)
		maps.Copy(tags, component.Tags)
		tags[`pantopic:module`] = ShardTypeQueue
		_, err := cluster.ShardCreate(&pb.Shard{
			Name: `queue`,
			Type: "pantopic/ext-raft",
			Tags: tags,
		})
		if err != nil {
			println(err)
			return cluster.StatusPending
		}
	}
	return cluster.StatusActive
}
