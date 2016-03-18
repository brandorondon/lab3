package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"



type Paxos struct {
    mu         sync.Mutex
    l          net.Listener
    dead       bool
    unreliable bool
    rpcCount   int
    peers      []string
    me         int // index into peers[]

    done_map map[int]int
    max_seen_seq int
    seq_map     map[int]Proposal
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
    c, err := rpc.Dial("unix", srv)
    if err != nil {
        err1 := err.(*net.OpError)
        if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
            fmt.Printf("paxos Dial() failed: %v\n", err1)
        }
        return false
    }
    defer c.Close()

    err = c.Call(name, args, reply)
    if err == nil {
        return true
    }

    fmt.Println(err)
    return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
    go px.garbageCollect() //start the node's garbage collect goroutine as soon
    go func() {
        seq := seq
        decided := px.seq_map[seq].decided
        curr_n := 0
        for !decided && !px.dead {
            max_n := -1
            curr_v := v
            prop_majority := 0
            for i := 0; i < len(px.peers); i++ {
                args := &PrepReq{seq, curr_n}
                reply := &PrepareReply{}
                if i == px.me{
                  px.Prepare(args,reply)
                }else{
                  call(px.peers[i],"Paxos.Prepare",args,reply)
                }
                if reply.Promised{
                  prop_majority ++
                  px.done_map[i] = reply.Done

                  if reply.N_a > max_n {
                      max_n = reply.N_a
                      curr_v = reply.Value
                  }
                }
            }
            //being accept phase if majoirty promised
            if prop_majority > len(px.peers)/2 {
            acc_majority := 0
            for i := 0; i < len(px.peers); i++ {
                args := &AcceptReq{seq, curr_n, curr_v}
                reply := &AcceptReply{}
                if i == px.me{
                  px.Accept(args,reply)
                } else {
                  call(px.peers[i],"Paxos.Accept",args,reply)
                }
                if reply.Accepted {
                    acc_majority++
                    px.done_map[i] = reply.Done
                }
            }
            //being decision phase if majoirty accepted
            if acc_majority > len(px.peers)/2 {
              for i := 0; i < len(px.peers); i++ {
                  args := &DecideReq{seq, curr_v}
                  reply := &DecideReply{}
                  if i == px.me {
                    px.Decide(args,reply)
                  } else {
                    call(px.peers[i],"Paxos.Decide",args,reply)
                  }
                  px.done_map[i] = reply.Done
              }
              break
            }
          //increase proposal nubmer if round fails to highest seen proposal #
          //or just monotically increase curr_n if not higher n is returned
          } else {
            if max_n > curr_n {
                curr_n = max_n + 1
            } else {
                curr_n++
            }
          }
        }
    }()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
    px.done_map[px.me] = seq
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
    return px.max_seen_seq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
    var min int
    for _, v := range px.done_map { //get a random value to start with from the map
      min = v
      break
    }
    for _, v := range px.done_map {
        if v < min {
            min = v
        }
    }
    return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
    if seq > px.max_seen_seq{
      px.max_seen_seq = seq
    }
    _, there := px.seq_map[seq]
    if there{
      return px.seq_map[seq].decided, px.seq_map[seq].value
    }
    return false, nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
    px.dead = true
    if px.l != nil {
        px.l.Close()
    }
}


func (px *Paxos) Prepare(args *PrepReq, reply *PrepareReply) error {
    px.mu.Lock()
    defer px.mu.Unlock()

    //update highest instance seen
    if args.Seq > px.max_seen_seq{
      px.max_seen_seq = args.Seq
    }
    //initialize instance state if first propose
    if _, there := px.seq_map[args.Seq]; !there {
        px.seq_map[args.Seq] = Proposal{-1, -1,false,nil}
    }

    state := px.seq_map[args.Seq]
    if args.N_a > state.n_p {
        px.seq_map[args.Seq] = Proposal{args.N_a, state.a_n,state.decided,state.value}
        reply.Value = state.value
        reply.N_a = state.a_n
        reply.Promised = true
    } else {
        reply.Promised = false
    }
    reply.Done = px.done_map[px.me]
    return nil
}


func (px *Paxos) Accept(args *AcceptReq, reply *AcceptReply) error {
    px.mu.Lock()
    defer px.mu.Unlock()
    state := px.seq_map[args.Seq]
    if args.A_n >= state.n_p {
        //update acceptor's state with new highest seen aceept
        px.seq_map[args.Seq] = Proposal{args.A_n, args.A_n,state.decided,args.Value}
        reply.A_n = args.Seq
        reply.Accepted = true
        //fmt.Println(px.me," accepted ",args.Value)
    } else {
        reply.Accepted = false
    }
    reply.Done = px.done_map[px.me]
    return nil
}

func (px *Paxos) Decide(args *DecideReq, reply *DecideReply) error {
    px.mu.Lock()
    defer px.mu.Unlock()
    n := px.seq_map[args.Seq].a_n
    px.seq_map[args.Seq] = Proposal{n, n, true, args.Value}
    reply.Done = px.done_map[px.me]
    return nil
}

//threaded to run periodically and delete instances that are no longer needed
func (px *Paxos) garbageCollect() {
    min := -1
    for !px.dead {
        global_min := px.Min()
        if min < global_min {
            for key, _ := range px.seq_map {
                if key < global_min {
                  px.seq_map[key] = Proposal{} //clear memory by reinitializing
                }
            }
            min = global_min
        }
        //wasn't sure exactly how long to sleep for, but larger numbers
        //gave me RPC count test failures
        time.Sleep(50 * time.Millisecond)
    }
}

func (px *Paxos) fill(m map[int]int, size int) map[int]int{
  for i := 0; i < size; i++ {
    m[i] = -1
  }
  return m
}
//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
    px := &Paxos{}
    px.peers = peers
    px.me = me
    ////
    px.max_seen_seq = -1
    px.seq_map = make(map[int]Proposal)
    px.done_map = px.fill(make(map[int]int),len(peers)) //populate map w/ -1's
    ////
    if rpcs != nil {
        // caller will create socket &c
        rpcs.Register(px)
    } else {
        rpcs = rpc.NewServer()
        rpcs.Register(px)

        // prepare to receive connections from clients.
        // change "unix" to "tcp" to use over a network.
        os.Remove(peers[me]) // only needed for "unix"
        l, e := net.Listen("unix", peers[me])
        if e != nil {
            log.Fatal("listen error: ", e)
        }
        px.l = l

        // please do not change any of the following code,
        // or do anything to subvert it.

        // create a thread to accept RPC connections
        go func() {
            for px.dead == false {
                conn, err := px.l.Accept()
                if err == nil && px.dead == false {
                    if px.unreliable && (rand.Int63()%1000) < 100 {
                        // discard the request.
                        conn.Close()
                    } else if px.unreliable && (rand.Int63()%1000) < 200 {
                        // process the request but force discard of reply.
                        c1 := conn.(*net.UnixConn)
                        f, _ := c1.File()
                        err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
                        if err != nil {
                            fmt.Printf("shutdown: %v\n", err)
                        }
                        px.rpcCount++
                        go rpcs.ServeConn(conn)
                    } else {
                        px.rpcCount++
                        go rpcs.ServeConn(conn)
                    }
                } else if err == nil {
                    conn.Close()
                }
                if err != nil && px.dead == false {
                    fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
                }
            }
        }()
    }
    return px
}

// MY STRUCTS
type Proposal struct {
    n_p int
    a_n  int
    decided   bool
    value     interface{}
}

type PrepReq struct {
    Seq int
    N_a  int
}

type PrepareReply struct {
    N_a int
    Value     interface{}
    Done      int
    Promised bool
}

type AcceptReq struct {
    Seq int
    A_n   int
    Value      interface{}
}

type AcceptReply struct {
    A_n int
    Done     int
    Accepted bool
}

type DecideReq struct {
    Seq int
    Value      interface{}
}

type DecideReply struct {
    Done  int
}
