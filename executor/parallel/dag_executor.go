package parallel

import (
	"sync"
)

// dagExecutor handles topologically traversing the DAG and scheduling concurrent processing for independent set of nodes
type dagExecutor struct {
	// Nodes in scheduled were enqueued for processing
	// and are waiting to be visited to enqueue their dependants.
	// Nodes get concurrently processed when calling processScheduled.
	scheduled []scheduledTask
	// Nodes that are currently processing.
	// Their status is either processed or not.
	processing processingQueue
	// Wait group that synchronizes processing node status
	// and unblocks when processing for all nodes completed.
	wg sync.WaitGroup
	// Mutex that controls access to visited and stale maps.
	mu sync.Mutex
	// Node that was scheduled for processing is marked as visited.
	visited []bool
	// Node that was already processed but requires re-execution is marked as stale.
	stale []bool
	// References to other dependant structs.
	dag        *DependencyDag
	txExecutor *transactionExecutor
	state      *accountDelta
}

func newDagExecutor(
	dag *DependencyDag,
	queue processingQueue,
	txExecutor *transactionExecutor,
	state *accountDelta,
) dagExecutor {
	return dagExecutor{
		visited:    make([]bool, len(dag.nodes)),
		stale:      make([]bool, len(dag.nodes)),
		dag:        dag,
		processing: queue,
		txExecutor: txExecutor,
		state:      state,
	}
}

func (e *dagExecutor) execute(nWorkers int) {

	for workerId := 0; workerId < nWorkers; workerId++ {
		go func(tasks <-chan processingTask) {
			for task := range tasks {
				e.processTask(task)
			}
		}(e.processing.tasks())
	}

	e.traverse()
}

func (e *dagExecutor) processTask(task processingTask) {
	node := e.dag.Node(task.seqId)

	if !task.isStale || len(node.Reads) == 0 {
		e.state.ApplyUpdates(node.SeqId, node.Updates)
		task.done()
		return
	}

	reExecutedNode := e.txExecutor.execute(e.state, node.SeqId, *node.Transaction)

	diff := e.dag.update(reExecutedNode)

	// The new Updates may affect new nodes in the DAG,
	// so we must revert the startState update and reprocess at a later point to compute the correct startState Updates.
	// Invalidate entire descendants subgraph,
	// because all the succeeding startState Updates may be invalid as well.
	e.dag.bfsFrom(diff.dependants.added, func(seqId int) bool {
		e.state.RevertUpdates(seqId, e.dag.Node(seqId).Updates)
		e.markUnvisited(seqId)

		// TODO(perf): Stop descend if this node was previously unvisited
		return true
	})

	e.dag.bfsFrom(diff.dependants.removed, func(seqId int) bool {
		e.state.RevertUpdates(seqId, e.dag.Node(seqId).Updates)
		e.markUnvisited(seqId)
		// This node may not be reachable from the current subgraph,
		// so we need a way to tell the e to re-execute it
		// alongside re-scheduling its processing (marking it unvisited).
		e.markStale(seqId)

		// TODO(perf): Stop descend if this node was previously unvisited?
		return true
	})

	if len(diff.dependencies.added) == 0 {
		e.state.ApplyUpdates(reExecutedNode.SeqId, reExecutedNode.Updates)
	} else {
		// This node was moved to a different part of the DAG
		// and should be processed again at a later point.
		e.markUnvisited(reExecutedNode.SeqId)
		for _, seqId := range diff.dependencies.added {
			e.markUnvisited(seqId)
		}
		e.state.RevertUpdates(node.SeqId, node.Updates)
	}

	task.done()
}

func (e *dagExecutor) traverse() {

	for seqId := range e.dag.nodes {
		// Only traverse the clusters of dependent transactions.
		// This will delay the execution of (initially) independent transactions
		// until (if) they are discovered to be part of one of the clusters.
		// This will minimize the re-execution times.
		if len(e.dag.Dependencies(seqId)) == 0 && len(e.dag.Dependants(seqId)) > 0 {
			e.schedule(seqId, false)
		}
	}

	e.processScheduled()

	for len(e.scheduled) > 0 {

		n := len(e.scheduled)
		for i := 0; i < n; i++ {
			seqId := e.scheduled[i].seqId

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
					e.schedule(depSeqId, true)
				}
			}
		}
		e.scheduled = e.scheduled[n:]

		e.processScheduled()
	}

	// Process leftover independent nodes that were not part of any dependency clusters.
	for seqId := range e.dag.nodes {
		if !e.visited[seqId] {
			// TODO: Can it happen that the node has descendant nodes that are not scheduled here?
			e.schedule(seqId, e.stale[seqId])
		}
	}

	e.processScheduled()

	e.processing.close()
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

func (e *dagExecutor) schedule(seqId int, isStale bool) {
	e.scheduled = append(e.scheduled, scheduledTask{seqId, isStale})
}

// Can be called concurrently for different SeqId values
func (e *dagExecutor) markUnvisited(seqId int) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.visited[seqId] = false
}

// Can be called concurrently for different SeqId values
func (e *dagExecutor) markStale(seqId int) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.visited[seqId] = true
}

func (e *dagExecutor) processScheduled() {
	if len(e.scheduled) > 0 {
		currBatch := make([]processingTask, 0, len(e.scheduled))
		for _, task := range e.scheduled {
			currBatch = append(currBatch, processingTask{
				seqId:   task.seqId,
				isStale: task.isStale,
				done:    e.wg.Done,
			})
		}
		e.wg.Add(len(currBatch))
		e.processing.enqueue(currBatch)
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
	seqId   int
	isStale bool
	done    func()
}

type scheduledTask struct {
	seqId   int
	isStale bool
}

// channelProcessingQueue is the default implementation of processingQueue using channels
type channelProcessingQueue struct {
	queue chan processingTask
}

func newChannelProcessingQueue() channelProcessingQueue {
	return channelProcessingQueue{queue: make(chan processingTask)}
}

func (q *channelProcessingQueue) enqueue(units []processingTask) {
	for _, unit := range units {
		q.queue <- unit
	}
}

func (q *channelProcessingQueue) tasks() <-chan processingTask {
	return q.queue
}

func (q *channelProcessingQueue) close() {
	close(q.queue)
}
