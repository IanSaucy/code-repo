package mapreduce

import (
	"io"
	"os"
	"errors"
	"sort"
	"encoding/json"
)

// 
// []KeyValue 排序的3个函数
//
type KeyValueSlice []KeyValue

func (a KeyValueSlice) Len() int {
	return len(a)
}

func (a KeyValueSlice) Less(i int, j int) bool {
	return a[i].Key < a[j].Key
}

func (a KeyValueSlice) Swap(i int, j int) {
	a[i], a[j] = a[j], a[i]
}

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	mylog("---------doReduce begin---------")
	mylog("jobName:", jobName)
	mylog("reduceTask:", reduceTask)
	mylog("outFile:", outFile)
	mylog("nMap:", nMap)
	
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
	kvList, err := readReduceFiles(jobName, reduceTask, nMap)
	if err != nil {
		mylog("readReduceFiles error ", err)
	}
	mylog("readReduceFiles len:%v", len(kvList))

	// 按照key排序
	sort.Sort(KeyValueSlice(kvList))

	// 把相同key的收集到一起
	keyList, valueList := resolveReduceData(kvList)

	// 调用reduceF
	reduceList := make([]string, len(keyList))
	for i, key := range keyList {
		result := reduceF(key, valueList[i])
		reduceList[i] = result
	}

	// 写到输出文件
	err = writeReduceByJson(outFile, keyList, reduceList)
	if err != nil {
		mylog("writeReduceByJson error ", err)
	}

	mylog("---------doReduce end---------")
}

func readReduceFiles(jobName string, reduceTask int, nMap int) ([]KeyValue, error) {
	kvList := make([]KeyValue, 0)
	
	for i := 0; i < nMap; i++ {
		rname := reduceName(jobName, i, reduceTask)
		rlist, err := readReduceFileByJson(rname)
		if err != nil {
			mylog("readReduceFileByJson error ", rname, err)
			return nil, err
		} else {
			kvList = append(kvList, rlist...)
		}
	}

	return kvList, nil
}

func readReduceFileByJson(fileName string) ([]KeyValue, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	kvList := make([]KeyValue, 0)
	decoder := json.NewDecoder(file)
	for {
		var kv KeyValue
		err := decoder.Decode(&kv)
		if err == io.EOF {
			break
		} else if err != nil {
			mylog("Decode error ", err)
			return nil, err
		} else {
			kvList = append(kvList, kv)
		}
	}

	return kvList, nil
}

func resolveReduceData(kvList []KeyValue) ([]string, [][]string) {
	keyList := make([]string, 0)
	valueList := make([][]string, 0)
	k  := -1

	for _, kv := range kvList {
		if k >= 0 && keyList[k] == kv.Key {
			valueList[k] = append(valueList[k], kv.Value)
		} else {
			k++
			keyList = append(keyList, kv.Key)
			valueList = append(valueList, []string{})
			valueList[k] = append(valueList[k], kv.Value)
		}
	}
	
	return keyList, valueList
}

func writeReduceByJson(fileName string, keyList []string, reduceList []string) error {
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
			mylog("Encode error ", err)
		}
	}

	return nil
}
