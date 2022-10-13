package ldappool

import (
	"net"
	"time"
)

type Server struct {
	*net.SRV
	Service string

	latency time.Duration

	// index in the priority queue
	index int
}

type srvHeap []*Server

func (pq srvHeap) Len() int {
	return len(pq)
}

func (pq srvHeap) Less(i, j int) bool {
	return pq[i].latency < pq[j].latency
}

func (pq srvHeap) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *srvHeap) Push(x any) {
	n := len(*pq)
	item := x.(*Server)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *srvHeap) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}
