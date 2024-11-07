package parallel

import (
	"fmt"
	"sync"
)

// dagExecutor handles topologically traversing the DAG and scheduling concurrent processingQueue for independent set of nodes
type dagExecutor struct {
	// Nodes in scheduled were enqueued for processingQueue
	// and are waiting to be visited to enqueue their dependants.
	// Nodes get concurrently processed when calling processScheduled.
	scheduled []scheduledTask
	processed []int
	// Nodes that are currently processing
	processingQueue processingQueue
	// Wait group that synchronizes processing node status
	// and unblocks when processing for all nodes completed.
	wg sync.WaitGroup
	// Mutex that controls access to visited and forceReExecution maps.
	mu sync.Mutex
	// Node that was scheduled for processing is marked as visited.
	visited []bool
	// Node that was already processed but requires re-execution is marked as forceReExecution.
	forceReExecution []bool
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
		visited:          make([]bool, len(dag.nodes)),
		forceReExecution: make([]bool, len(dag.nodes)),
		dag:              dag,
		processingQueue:  queue,
		txExecutor:       txExecutor,
		state:            state,
	}
}

func (e *dagExecutor) execute(nWorkers int) {

	for workerId := 0; workerId < nWorkers; workerId++ {
		go func(tasks <-chan processingTask) {
			for task := range tasks {
				e.processTask(task)
			}
		}(e.processingQueue.tasks())
	}

	e.traverse()
}

func (e *dagExecutor) processTask(task processingTask) {
	node := e.dag.Node(task.seqId)

	e.state.RevertUpdates(node.SeqId)

	if !task.forceReExecution || len(node.Reads) == 0 {
		e.state.ApplyUpdates(node.SeqId, node.Updates)
		task.done()
		return
	}

	reExecutedNode := e.txExecutor.execute(e.state, node.SeqId, *node.Transaction)

	diff := e.dag.update(reExecutedNode)

	e.reExecuteSubgraphFrom(diff.dependants.added)
	e.reExecuteSubgraphFrom(diff.dependants.removed)
	// TODO: This is failing because nodes may be duplicated in the scheduled queue
	if len(diff.dependencies.added) > 0 {
		e.reExecuteSubgraphFrom([]int{node.SeqId})
	} else {
		e.state.ApplyUpdates(reExecutedNode.SeqId, reExecutedNode.Updates)
	}

	task.done()
}

func (e *dagExecutor) traverse() {

	for seqId := range e.dag.nodes {
		// Only traverse the clusters of dependent transactions.
		// This will delay the execution of (initially) independent transactions
		// until (if) they are discovered to be part of one of the clusters to minimize unnecessary re-execution.
		if len(e.dag.Dependencies(seqId)) == 0 && len(e.dag.Dependants(seqId)) > 0 {
			e.schedule(seqId, false)
		}
	}

	e.processScheduled()

	for len(e.processed) > 0 {

		for _, seqId := range e.processed {

			// Stop with further sub-graph traversal from the current node,
			// because the current node has new dependencies,
			// which must be reprocessed first.
			if len(e.unvisitedDependencies(seqId)) > 0 {
				// TODO: What if the dependencies are not reachable anymore?
				// We can safely skip this node,
				// since the path from one of the new Dependencies
				// will lead to this node eventually.
				continue
			}

			dependants := e.unvisitedDependants(seqId)
			e.visited[seqId] = true

			for _, depSeqId := range dependants {
				// Only schedule dependents with all their dependencies processed.
				// Skipped dependents will be eventually reached from one of its unvisited dependencies.
				//
				// Example graph:
				//  A     B
				//  │     │
				//  ▼     ▼
				//  C ──► D
				// When we process A and B we'll want to schedule their dependants C and D,
				// but we must not schedule D in the same batch as C,
				// since C needs to be processed sequentially before D.
				// So we delay D execution and schedule it in the next iteration once we process C.
				if len(e.unvisitedDependencies(depSeqId)) == 0 {
					e.schedule(depSeqId, true)
				}
			}
		}
		e.processScheduled()
	}

	// Process leftover independent nodes that are not part of any dependency clusters.
	for seqId := range e.dag.nodes {
		if !e.visited[seqId] {
			e.schedule(seqId, e.forceReExecution[seqId])
		}
	}

	e.processScheduled()

	e.processingQueue.close()
}

func (e *dagExecutor) reExecuteSubgraphFrom(seqIds []int) {
	// The given sub-graphs may or may not be connected to the ones we are currently processingQueue,
	// so we need to make sure to add the new disconnected sub-graphs to the queue,
	// so that we'll visit and process them in the next steps.
	for _, seqId := range seqIds {
		// TODO: Do we also need to instead check if there exists a path from one of our scheduled ones to seqId?
		if len(e.unvisitedDependencies(seqId)) == 0 {
			e.schedule(seqId, true)
		}
	}

	e.dag.bfsFrom(seqIds, func(seqId int) {
		e.reVisit(seqId)
		e.reExecute(seqId)
		e.state.RevertUpdates(seqId)
	})
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

func (e *dagExecutor) schedule(seqId int, forceReExecution bool) {
	for _, scheduled := range e.scheduled {
		if scheduled.seqId == seqId {
			panic(fmt.Sprintf("node %d was already scheduled", seqId))
		}
	}
	e.scheduled = append(e.scheduled, scheduledTask{seqId, forceReExecution})
}

// Can be called concurrently for different SeqId values
func (e *dagExecutor) reVisit(seqId int) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.visited[seqId] = false
}

// Can be called concurrently for different SeqId values
func (e *dagExecutor) reExecute(seqId int) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.forceReExecution[seqId] = true
}

func (e *dagExecutor) processScheduled() {
	e.processed = make([]int, 0, len(e.scheduled))

	if len(e.scheduled) > 0 {
		currBatch := make([]processingTask, 0, len(e.scheduled))
		for _, task := range e.scheduled {
			currBatch = append(currBatch, processingTask{
				seqId:            task.seqId,
				forceReExecution: task.forceReExecution,
				done:             e.wg.Done,
			})
		}
		e.wg.Add(len(currBatch))
		e.processingQueue.enqueue(currBatch)
		e.wg.Wait()

		for _, task := range e.scheduled {
			e.processed = append(e.processed, task.seqId)
		}
		e.scheduled = make([]scheduledTask, 0)
	}
}

func (e *dagExecutor) Graphviz() string {
	graphViz := Graphviz{
		Name:             "DependencyGraph",
		NodeFillColor:    NewRgbColor(142, 202, 230),
		NodeLabelColor:   NewRgbColor(2, 48, 71),
		NodeOutlineColor: NewRgbColor(255, 183, 3),
		EdgeLabelColor:   NewRgbColor(33, 158, 188),
		EdgeFillColor:    NewRgbColor(2, 48, 71),
		GetNodeLabel: func(seqId int) string {
			return fmt.Sprintf("\"%d (%d)\"", seqId, e.txExecutor.executionCount[seqId])
		},
	}
	return graphViz.Generate(e.dag)
}

// processingQueue abstracts away the queue implementation (done with channels in channelProcessingQueue)
// so that it's easier to test the batches that are enqueued at different steps.
type processingQueue interface {
	enqueue([]processingTask)
	close()
	tasks() <-chan processingTask
}

type processingTask struct {
	seqId            int
	forceReExecution bool
	done             func()
}

type scheduledTask struct {
	seqId            int
	forceReExecution bool
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
