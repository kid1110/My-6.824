package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type MapF func(string, string) []KeyValue
type ReduceF func(string, []string) string

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	log.SetOutput(ioutil.Discard)
	for {
		time.Sleep(1 * time.Second)
		task := Task{}

		call("Coordinator.StartTask", &PlaceHolder{}, &task)
		if task.Operation == TaskWait {
			continue
		}
		if task.IsMap {
			log.Printf("received map job %s", task.Map.Filename)
			err := MapHandler(task, mapf)
			if err != nil {
				log.Fatal(err.Error())
				return
			}

		} else {
			log.Printf("received reduce job %d %v", task.Reduce.Id, task.Reduce.InterMediaFile)
			err := ReduceHandler(task, reducef)
			if err != nil {
				log.Fatal(err.Error())
				return
			}
		}

	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}
func MapHandler(task Task, mapf MapF) error {
	filename := task.Map.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("cannot open %v", filename)
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal("cannot read content %v", err)
		return err
	}
	defer file.Close()
	kva := mapf(filename, string(content))
	var encoders []*json.Encoder
	for i := 0; i < task.NReduce; i++ {
		f, err := os.Create(fmt.Sprintf("mr-%d-%d", task.Map.Id, i))
		if err != nil {
			log.Fatal("cannot create intermediate file")
		}

		encoders = append(encoders, json.NewEncoder(f))
	}
	for _, kv := range kva {
		encoders[ihash(kv.Key)%task.NReduce].Encode(&kv)
	}
	call("Coordinator.Finished", &FinishArgs{IsMap: true, Id: task.Map.Id}, &PlaceHolder{})
	return nil
}
func ReduceHandler(task Task, Reduce ReduceF) error {
	var kva []KeyValue
	for _, filename := range task.Reduce.InterMediaFile {
		iFile, err := os.Open(filename)
		if err != nil {
			log.Fatal("cannot open intermeidate file")
			return err
		}
		dec := json.NewDecoder(iFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		err = iFile.Close()
		if err != nil {
			log.Fatal("cannot close %v", filename)
			return err
		}
	}
	sort.Sort(ByKey(kva))
	NewName := fmt.Sprintf("mr-out-%d", task.Reduce.Id)
	temp, err := os.CreateTemp(".", NewName)
	if err != nil {
		log.Fatalf("cannot create reduce result tempfile %s", NewName)
		return err
	}
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		num := Reduce(kva[i].Key, values)
		fmt.Fprintf(temp, "%v %v\n", kva[i].Key, num)
		i = j

	}
	err = os.Rename(temp.Name(), NewName)
	if err != nil {
		return err
	}
	call("Coordinator.Finished", &FinishArgs{IsMap: false, Id: task.Reduce.Id}, &PlaceHolder{})
	return nil
}

type ByKey []KeyValue

func (a ByKey) Len() int {
	return len(a)
}
func (a ByKey) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a ByKey) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
