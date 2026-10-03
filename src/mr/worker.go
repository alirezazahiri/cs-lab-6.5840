package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// KeyValue is one intermediate key/value pair.
type KeyValue struct {
	Key   string
	Value string
}

// ihash chooses the reduce partition for a key.
func ihash(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string

// Worker repeatedly requests and executes tasks until the coordinator says
// the job is complete.
func Worker(
	sockname string,
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	coordSockName = sockname

	for {
		args := RequestTaskArgs{}
		reply := RequestTaskReply{}

		if !call("Coordinator.RequestTask", &args, &reply) {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		switch reply.Type {
		case MapTask:
			if runMapTask(reply, mapf) {
				reportTask(reply.Type, reply.TaskID, reply.Generation)
			}
		case ReduceTask:
			if runReduceTask(reply, reducef) {
				reportTask(reply.Type, reply.TaskID, reply.Generation)
			}
		case WaitTask:
			time.Sleep(500 * time.Millisecond)
		case ExitTask:
			return
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func runMapTask(
	task RequestTaskReply,
	mapf func(string, string) []KeyValue,
) bool {
	content, err := os.ReadFile(task.File)
	if err != nil {
		log.Printf("worker: read input %q: %v", task.File, err)
		return false
	}

	keyValues := mapf(task.File, string(content))
	if task.NReduce == 0 {
		return true
	}

	partitions := make([][]KeyValue, task.NReduce)
	for _, kv := range keyValues {
		partition := ihash(kv.Key) % task.NReduce
		partitions[partition] = append(partitions[partition], kv)
	}

	// Write temporary files first, then rename them to their final names.
	// This prevents reducers from seeing partially written intermediate data.
	tempNames := make([]string, task.NReduce)
	finalNames := make([]string, task.NReduce)

	for reduceID, partition := range partitions {
		finalNames[reduceID] = fmt.Sprintf("mr-%d-%d", task.TaskID, reduceID)

		file, err := os.CreateTemp(".", "mr-map-tmp-*")
		if err != nil {
			log.Printf("worker: create map temp file: %v", err)
			removeFiles(tempNames)
			return false
		}

		tempNames[reduceID] = file.Name()
		encoder := json.NewEncoder(file)

		writeErr := error(nil)
		for _, kv := range partition {
			if err := encoder.Encode(&kv); err != nil {
				writeErr = err
				break
			}
		}

		closeErr := file.Close()
		if writeErr != nil || closeErr != nil {
			log.Printf("worker: write map output: %v", firstError(writeErr, closeErr))
			removeFiles(tempNames)
			return false
		}
	}

	for i := range tempNames {
		if err := os.Rename(tempNames[i], finalNames[i]); err != nil {
			log.Printf("worker: publish map output %q: %v", finalNames[i], err)
			removeFiles(tempNames)
			return false
		}
	}

	return true
}

func runReduceTask(
	task RequestTaskReply,
	reducef func(string, []string) string,
) bool {
	valuesByKey := make(map[string][]string)

	for mapID := 0; mapID < task.NMap; mapID++ {
		filename := fmt.Sprintf("mr-%d-%d", mapID, task.TaskID)
		file, err := os.Open(filename)
		if err != nil {
			log.Printf("worker: open intermediate file %q: %v", filename, err)
			return false
		}

		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := decoder.Decode(&kv)
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = file.Close()
				log.Printf("worker: decode intermediate file %q: %v", filename, err)
				return false
			}
			valuesByKey[kv.Key] = append(valuesByKey[kv.Key], kv.Value)
		}

		if err := file.Close(); err != nil {
			log.Printf("worker: close intermediate file %q: %v", filename, err)
			return false
		}
	}

	keys := make([]string, 0, len(valuesByKey))
	for key := range valuesByKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	tempFile, err := os.CreateTemp(".", "mr-reduce-tmp-*")
	if err != nil {
		log.Printf("worker: create reduce temp file: %v", err)
		return false
	}

	tempName := tempFile.Name()
	finalName := fmt.Sprintf("mr-out-%d", task.TaskID)

	for _, key := range keys {
		result := reducef(key, valuesByKey[key])
		if _, err := fmt.Fprintf(tempFile, "%v %v\n", key, result); err != nil {
			_ = tempFile.Close()
			_ = os.Remove(tempName)
			log.Printf("worker: write reduce output: %v", err)
			return false
		}
	}

	if err := tempFile.Close(); err != nil {
		_ = os.Remove(tempName)
		log.Printf("worker: close reduce output: %v", err)
		return false
	}

	if err := os.Rename(tempName, finalName); err != nil {
		_ = os.Remove(tempName)
		log.Printf("worker: publish reduce output %q: %v", finalName, err)
		return false
	}

	return true
}

func reportTask(taskType TaskType, taskID, generation int) {
	args := ReportTaskArgs{
		Type:       taskType,
		TaskID:     taskID,
		Generation: generation,
	}
	reply := ReportTaskReply{}

	if !call("Coordinator.ReportTask", &args, &reply) {
		log.Printf("worker: failed to report task %d completion", taskID)
	}
}

func removeFiles(files []string) {
	for _, filename := range files {
		if filename != "" {
			_ = os.Remove(filename)
		}
	}
}

func firstError(first, second error) error {
	if first != nil {
		return first
	}
	return second
}

// call sends an RPC request to the coordinator.
func call(rpcname string, args interface{}, reply interface{}) bool {
	client, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Printf("worker: dialing coordinator: %v", err)
		return false
	}
	defer client.Close()

	if err := client.Call(rpcname, args, reply); err != nil {
		log.Printf("worker: RPC %s failed: %v", rpcname, err)
		return false
	}
	return true
}
