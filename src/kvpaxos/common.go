package kvpaxos

import "hash/fnv"

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
)
type Err string


type PutArgs struct {
  // You'll have to add definitions here.
  Key string
  Value string
  DoHash bool  // For PutHash
  // You'll have to add definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.

  //Add Unique ID Field?
}

type PutReply struct {
  Err Err
  PreviousValue string   // For PutHash
  Partitioned bool
}

type GetArgs struct {
  Key string
  //Add Unieque ID Field?
  // You'll have to add definitions here.
}

type GetReply struct {
  Err Err
  Value string
  Partitioned bool
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}
