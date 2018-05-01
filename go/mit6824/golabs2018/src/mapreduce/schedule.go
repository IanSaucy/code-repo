package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	// 遍历所有任务，获取可用的worker，发起RPC任务
	var waitGroup sync.WaitGroup

	waitGroup.Add(ntasks)

	for i := 0; i < ntasks; i++ {
		go doSchedule(jobName, mapFiles, n_other, phase, registerChan, i, &waitGroup)
	}

	fmt.Printf("waitGroup Wait\n")
	waitGroup.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
}

func doSchedule(jobName string, mapFiles []string, n_other int, phase jobPhase, registerChan chan string, task int, waitGroup *sync.WaitGroup) {
	workerAddress := getWorkerAddress(registerChan)

	taskArgs := DoTaskArgs{
		JobName:       jobName,
		Phase:         phase,
		TaskNumber:    task,
		NumOtherPhase: n_other}
	if phase == mapPhase {
		taskArgs.File = mapFiles[task]
	}

	flag := call(workerAddress, "Worker.DoTask", taskArgs, nil)
	if flag {
		fmt.Printf("DoTask %d success\n", taskArgs.TaskNumber)

		waitGroup.Done()

		go func(address string) {
			fmt.Printf("begin return worker address to registerChan %s\n", workerAddress)
			registerChan <- address
			fmt.Printf("end return worker address to registerChan %s\n", workerAddress)
		}(workerAddress)
	} else {
		fmt.Printf("DoTask %d fail\n", taskArgs.TaskNumber)

		go doSchedule(jobName, mapFiles, n_other, phase, registerChan, task, waitGroup)
	}
}

func getWorkerAddress(registerChan chan string) string {
	//fmt.Printf("getWorkerAddress\n")
	workerAddress := <-registerChan
	fmt.Printf("get worker address from registerChan %s\n", workerAddress)
	return workerAddress
}

func doWorkerTask(workerAddress string, taskArgs DoTaskArgs, registerChan chan string, waitGroup *sync.WaitGroup) {


	
}
