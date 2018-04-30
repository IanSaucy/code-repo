package mapreduce

import (
	"os"
	"errors"
	"sort"
	"encoding/json"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	debug("---------doReduce begin---------\n")
	debug("jobName:%v\n", jobName)
	debug("reduceTask:%v\n", reduceTask)
	debug("outFile:%v\n", outFile)
	debug("nMap:%v\n", nMap)
	
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	// 读取所有map任务中生成的，跟reduceTask相关的文件
	var kvList []KeyValue = readReduceFiles(jobName, reduceTask, nMap)

	// 按照key排序
	sort.Sort(kvList)

	// 把相同key的收集到一起
	keyList, valueList := resolveReduceData(kvList)

	// 调用reduceF
	reduceList := make([]string, len(keyList))
	for i, key := range keyList {
		result := reduceF(key, valueList[i])
		reduceList = append(reduceList, result)
	}

	// 写到输出文件
	err := writeReduceByJson(keyList, reduceList)
	if err != nil {
		debug("writeReduceByJson error %v\n", err)
	}

	debug("---------doReduce end---------\n")
}

func readReduceFiles(jobName string, reduceTask int, nMap int) ([]KeyValue, error) {
	kvList := make([]KeyValue, 0)
	
	for i := 0; i < nMap; i++ {
		rname := reduceName(jobName, i, reduceTask)
		err := readReduceFileByJson(rname, kvList)
		if err != nil {
			debug("readReduceFileByJson %s error %v\n", rname, err)
			return nil, err
		}
	}

	return kvList, nil
}

func readReduceFileByJson(fileName string, kvList []KeyValue) error {
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer file.Close()
	
	decoder := json.NewDecoder(file)
	for {
		var kv KeyValue
		err := decoder.Decode(&kv)
		if err == io.EOF {
			break
		} else if err != nil {
			debug("Decode error %v\n", err)
			return err
		} else {
			kvList = appen(kvList, kv)
		}
	}
}

func resolveReduceData(kvList []KeyValue) ([]string, [][]string) {
	keyList := make([]string, 0)
	valueList := make([][]string, 0)
	k  := -1

	for kv := range kvList {
		if k >= 0 && keyList[k] == kv.Key {
			valueList[k] = append(valueList[k], kv.Value)
		} else {
			k++
			keyList = append(keyList, kv.Key)
			valueList[k] = append(valueList[k], kv.Value)
		}
	}
	
	return keyList, valueList
}

func writeReduceByJson(fileName string, keyList []string, reductList []string) error {
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()
	
	encoder := json.NewEncoder(file)
	if encoder == nil {
		return errors.New("NewEncoder return nil")
	}
	for i, key := range keyList {
		err := encoder.Encode(KeyValue{key, reduceList[i]})
		if err != nil {
			debug("Encode error %v\n", err)
		}
	}

	return nil
}
