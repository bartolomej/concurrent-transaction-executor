package parallel

import (
	"sync"
)

// dagExecutor handles topologically traversing the DAG and scheduling concurrent processing for independent set of nodes
type dagExecutor struct {
	// nodes in scheduledQueue were already processed
	// and are waiting to be traversed to enqueue their Dependants
	scheduledQueue  []int
	processingQueue processingQueue
	wg              sync.WaitGroup
	mu              sync.Mutex
	visited         []bool
	dag             *DependencyDag
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
		// Only traverse the clusters of dependent transactions.
		// This will delay the execution of (initially) independent transactions
		// until (if) they are discovered to be part of one of the clusters.
		// This will minimize the re-execution times.
		if len(e.dag.Dependencies(seqId)) == 0 && len(e.dag.Dependants(seqId)) > 0 {
			e.schedule(seqId)
		}
	}

	e.awaitProcessing()

	for len(e.scheduledQueue) > 0 {

		n := len(e.scheduledQueue)
		for i := 0; i < n; i++ {
			seqId := e.scheduledQueue[i]

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
		e.scheduledQueue = e.scheduledQueue[n:]

		e.awaitProcessing()
	}

	// Process leftover independent nodes that were not part of any dependency clusters.
	for seqId := range e.dag.nodes {
		if !e.visited[seqId] {
			e.schedule(seqId)
		}
	}

	e.awaitProcessing()

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
	e.scheduledQueue = append(e.scheduledQueue, seqId)
}

func (e *dagExecutor) isScheduled(seqId int) bool {
	for _, scheduledSeqId := range e.scheduledQueue {
		if scheduledSeqId == seqId {
			return true
		}
	}
	return false
}

// Can be called concurrently for different SeqId values
func (e *dagExecutor) markUnvisited(seqId int) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.visited[seqId] = false
}

func (e *dagExecutor) awaitProcessing() {
	if len(e.scheduledQueue) > 0 {
		currBatch := make([]processingTask, 0, len(e.scheduledQueue))
		for _, seqId := range e.scheduledQueue {
			currBatch = append(currBatch, processingTask{
				nodeSeqId: seqId,
				done: func() {
					e.wg.Done()
				},
			})
		}
		e.wg.Add(len(currBatch))
		e.processingQueue.enqueue(currBatch)
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
