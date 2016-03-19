package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"


const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}


type Op struct {
  // Your definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  Operation string
  Key string
  Value string
  Result string
  Op_ID int64
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Your definitions here.
  kv_store map[string]string
  curr_seq int
  highest_op int
  serv_requests map[int64]int64
  responses map[int64]string
}

func(kv *KVPaxos) wait_for_agree(seq int) bool{
  to := 10 * time.Millisecond
  for {
    decided, _ := kv.px.Status(seq)
    if decided {
      return true
    }
    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }
  return false
}

func(kv *KVPaxos) check_for_holes(seq int){
  hole := false
  for i := kv.px.Min(); i <= kv.px.Max() ; i++{
    completed, _ := kv.px.Status(i)
    if !completed {
      hole = true
      kv.px.Start(i,Op{"","","","",-1})
    }
  }
  if hole {
    time.Sleep(10 * time.Millisecond)
    kv.check_for_holes(seq)
  }
  return
}

func(kv *KVPaxos) complete(seq int) string{

  var old_val string
  for i := kv.px.Min(); i <= seq ; i++{
    decided, val := kv.px.Status(i)
    if decided {
      op := val.(Op)
      key := val.(Op).Key
      value := val.(Op).Value

      old_val = ""
      _,there := kv.kv_store[key]

      if there{
        old_val = kv.kv_store[key]
      }
      if op.Operation == "PutHash"{

        new_val := NextValue(old_val,value)
        kv.kv_store[key] = new_val

      } else if op.Operation == "Put"{
        kv.kv_store[key] = value

      }
      kv.highest_op++
      //kv.px.Done(i) <---- giving fatal map problem
    }
  }
  return old_val
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()

  operation := Op{"Get",args.Key,"","",args.Id}

  last_serv_req, there:= kv.serv_requests[args.Serv_id]
  if there && last_serv_req == args.Id{
    //fmt.Println("*******&******* REPEAT *****&********")
    reply.Value = kv.responses[args.Serv_id]
    return nil
  }

  done:= false
  seq := kv.px.Max()+1

  for !done {
    kv.px.Start(seq, operation)
    agreed := kv.wait_for_agree(seq)
    if !agreed {
      reply.Partitioned = true
      return nil
    } else {
      _, decided_val := kv.px.Status(seq)
      success := (decided_val == operation)
      if success {
        kv.check_for_holes(seq)
        response := kv.complete(seq)
        kv.responses[args.Serv_id] = response
        reply.Value = response
        done = true
        return nil
      }
    }
    seq = kv.px.Max()+1
  }
  return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()

  operation := Op{"Put",args.Key,args.Value,"",args.Id}
  if args.DoHash {
    operation.Operation = "PutHash"
  }

  done:= false
  //duplicate request
  last_serv_req, there := kv.serv_requests[args.Serv_id]
  if there && last_serv_req == args.Id {
    reply.PreviousValue = kv.responses[args.Serv_id]
    return nil
  }

  seq := kv.px.Max()+1
  for !done {

    kv.px.Start(seq, operation)
    agreed := kv.wait_for_agree(seq)
    if !agreed {
      reply.Partitioned = true
      return nil
    } else {
      _, decided_val := kv.px.Status(seq)
      success := (decided_val == operation)
      if success {
        kv.check_for_holes(seq)
        response := kv.complete(seq)
        kv.serv_requests[args.Serv_id] = args.Id
        kv.responses[args.Serv_id] = response
        reply.PreviousValue = response
        //kv.px.Done(kv.highest_op-1)
        done = true
        kv.curr_seq = seq
        return nil
      }
    }
    seq = kv.px.Max()+1
  }
  return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
  DPrintf("Kill(%d): die\n", kv.me)
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
  // this call is all that's needed to persuade
  // Go's RPC library to marshall/unmarshall
  // struct Op.
  gob.Register(Op{})

  kv := new(KVPaxos)
  kv.me = me

  // Your initialization code here.
  kv.kv_store = make(map[string]string)
  kv.curr_seq = -1
  kv.highest_op = 0
  kv.serv_requests = make(map[int64]int64)
  kv.responses = make(map[int64]string)
  //

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l


  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && kv.dead == false {
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  return kv
}
