package parallel

import "sync"

// dagExecutor handles topologically traversing the DAG and scheduling concurrent processing for independent set of nodes
type dagExecutor struct {
	// nodes in q were already processed
	// and are waiting to be traversed to process their dependants
	q               []int
	wg              sync.WaitGroup
	currBatch       []processingTask
	visited         []bool
	dag             *dependencyDag
	processingQueue processingQueue
}

func newDagExecutor(dag *dependencyDag, queue processingQueue) dagExecutor {
	return dagExecutor{
		visited:         make([]bool, len(dag.nodes)),
		dag:             dag,
		processingQueue: queue,
	}
}

// TODO(perf): If channel queue scheduling is a bottleneck, pushing a batch of tasks to chan processingTask[] instead
func (e *dagExecutor) execute() {

	for seqId := range e.dag.nodes {
		if len(e.dag.dependencies(seqId)) == 0 {
			e.schedule(seqId)
		}
	}

	e.awaitProcessing()

	for len(e.q) > 0 {

		n := len(e.q)
		for i := 0; i < n; i++ {
			seqId := e.q[i]

			// Dependencies changed since last processing, delay removal.
			// Node will be processed again at some future point,
			// since it now depends on some other node that has yet to be traversed.
			if len(e.unvisitedDependencies(seqId)) > 0 {
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
	for _, depSeqId := range e.dag.dependencies(seqId) {
		if !e.visited[depSeqId] {
			unvisited = append(unvisited, depSeqId)
		}
	}
	return unvisited
}

func (e *dagExecutor) unvisitedDependants(seqId int) []int {
	unvisited := make([]int, 0)
	for _, depSeqId := range e.dag.dependants(seqId) {
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
		markUnvisited: func(seqId int) {
			// Can be called concurrently for different nodeSeqId values
			e.visited[seqId] = false
		},
	})
	e.q = append(e.q, seqId)
}

func (e *dagExecutor) awaitProcessing() {
	if len(e.currBatch) > 0 {
		e.processingQueue.process(e.currBatch)
		e.currBatch = make([]processingTask, 0)
		e.wg.Wait()
	}
}
