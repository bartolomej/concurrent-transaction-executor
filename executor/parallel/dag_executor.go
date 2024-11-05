package parallel

import (
	"sync"
)

// dagExecutor handles topologically traversing the DAG and scheduling concurrent processing for independent set of nodes
type dagExecutor struct {
	// nodes in q were already processed
	// and are waiting to be traversed to enqueue their Dependants
	q               []int
	wg              sync.WaitGroup
	currBatch       []processingTask
	visited         []bool
	dag             *DependencyDag
	processingQueue processingQueue
}

func newDagExecutor(dag *DependencyDag, queue processingQueue) dagExecutor {
	return dagExecutor{
		visited:         make([]bool, len(dag.nodes)),
		dag:             dag,
		processingQueue: queue,
	}
}

func (e *dagExecutor) execute() {

	for seqId := range e.dag.nodes {
		if len(e.dag.Dependencies(seqId)) == 0 {
			e.schedule(seqId)
		}
	}

	e.awaitProcessing()

	for len(e.q) > 0 {

		n := len(e.q)
		for i := 0; i < n; i++ {
			seqId := e.q[i]

			// Stop with further sub-graph traversal from the current node,
			// because the current node has new Dependencies,
			// which must be reprocessed first.
			if len(e.unvisitedDependencies(seqId)) > 0 {
				// We can safely skip this node,
				// since the path from one of the new Dependencies
				// will lead to this node eventually.
				continue
			}

			dependants := e.unvisitedDependants(seqId)
			e.visited[seqId] = true

			for _, depSeqId := range dependants {
				if len(e.unvisitedDependencies(depSeqId)) == 0 {
					e.schedule(depSeqId)
				}
			}
		}
		e.q = e.q[n:]

		e.awaitProcessing()
	}

	e.processingQueue.close()
}

func (e *dagExecutor) unvisitedDependencies(seqId int) []int {
	unvisited := make([]int, 0)
	for _, depSeqId := range e.dag.Dependencies(seqId) {
		if !e.visited[depSeqId] {
			unvisited = append(unvisited, depSeqId)
		}
	}
	return unvisited
}

func (e *dagExecutor) unvisitedDependants(seqId int) []int {
	unvisited := make([]int, 0)
	for _, depSeqId := range e.dag.Dependants(seqId) {
		if !e.visited[depSeqId] {
			unvisited = append(unvisited, depSeqId)
		}
	}
	return unvisited
}

func (e *dagExecutor) schedule(seqId int) {
	e.wg.Add(1)
	e.currBatch = append(e.currBatch, processingTask{
		nodeSeqId: seqId,
		done: func() {
			e.wg.Done()
		},
	})
	e.q = append(e.q, seqId)
}

// Can be called concurrently for different SeqId values
func (e *dagExecutor) markUnvisited(seqId int) {
	e.visited[seqId] = false
}

func (e *dagExecutor) awaitProcessing() {
	if len(e.currBatch) > 0 {
		e.processingQueue.enqueue(e.currBatch)
		e.currBatch = make([]processingTask, 0)
		e.wg.Wait()
	}
}

// processingQueue abstracts away the queue implementation (done with channels in channelProcessingQueue)
// so that it's easier to test the batches that are enqueued at different steps.
type processingQueue interface {
	enqueue([]processingTask)
	close()
	tasks() <-chan processingTask
}

type processingTask struct {
	nodeSeqId int
	done      func()
}
