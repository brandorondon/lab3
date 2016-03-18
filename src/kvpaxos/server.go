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
  //op_log map[seq]Op{} Paxos's seq_map is the log already isnt?
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
  for i := 0 ; i < seq ; i++{
    completed, _ := kv.px.Status(i)
    if !completed {
      hole = true
      kv.px.Start(i,Op{"","",""})
    }
  }
  if hole {
    time.Sleep(10 * time.Millisecond)
    kv.check_for_holes(seq)
  }
}

func(kv *KVPaxos) complete(seq int){
  for i := 0; i < seq ; i++{
    there, val := kv.px.Status(i)
    fmt.Println(val)
    if there {
      op := val.(Op).Operation
      if op == "Put" {
        key := val.(Op).Key
        value := val.(Op).Value
        kv.kv_store[key] = value
      }
    }
  }
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  //kv.mu.Lock()
  //defer kv.mu.Unlock()
  fmt.Println("get on server: ", kv.me)
  fmt.Println(kv.kv_store)
  operation := Op{"Get",args.Key,""}
  done:= false
  seq := kv.curr_seq
  for !done {
    kv.mu.Lock()
    if kv.px.Max() > kv.curr_seq {
      seq = kv.px.Max() + 1
    } else {
      seq = kv.curr_seq + 1
    }
    kv.mu.Unlock()
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
        kv.complete(seq)
        done = true
        fmt.Println(args.Key)
        reply.Value = kv.kv_store[args.Key]
        kv.curr_seq = seq
        return nil
        }
    }
    fmt.Println("serv stuck on  get seq: ",seq," with key: ",args.Key)
  }
  return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  //kv.mu.Lock()
  //defer kv.mu.Unlock()
  operation := Op{"Put",args.Key,args.Value}
  done:= false
  seq := kv.curr_seq
  reply.PreviousValue = ""
  for !done {
    kv.mu.Lock()
    if kv.px.Max() > kv.curr_seq {
      seq = kv.px.Max() + 1
    } else {
      seq = kv.curr_seq + 1
    }
    kv.mu.Unlock()
    kv.px.Start(seq, operation)
    agreed := kv.wait_for_agree(seq)
    if !agreed {
      reply.Partitioned = true
      return nil
    } else {
      _, decided_val := kv.px.Status(seq)
      success := (decided_val == operation)
      if success {
        if args.DoHash {
          reply.PreviousValue = kv.kv_store[args.Key]
        }
        kv.kv_store[args.Key] = args.Value
        done = true
        kv.curr_seq = seq
        
        fmt.Println("server: ", kv.me)
        fmt.Println(kv.kv_store)
        return nil
        }
      }
      fmt.Println("serv stuck on seq: ",seq," with value: ",args.Value)
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
