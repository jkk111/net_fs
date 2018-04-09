package main

import (
  "./inc"
  "./filestore"
  "fmt"
  "encoding/json"
  "net/http"
  "io"
  // "io/ioutil"
)

const (
  SIZE = 5 * 1024 * 1024 * 1024
)

// 192.168.30.140:8080/ws

var (
  bootstrap = []string {}
  store * filestore.FileStore
  ENOENT = json_error("ENOENT")
  ENOTDIR = json_error("ENOTDIR")
  router * inc.INCRouter = inc.NewINCRouter(":8080", bootstrap)
)

func json_error(etype string) []byte {
  str := `{"success":"false","error":"` + etype + "\"}"
  return []byte(str)
}

type InvalidationRequest struct {
  Name string `json:"name"`
  User string `json:"user"`
  Version int64 `json:"version"`
}

type ReadRequest struct {
  Name string `json:"name"`
  User string `json:"user"`
  Offset int64 `json:"offset"`
  Length int64 `json:"length"`
  Version int64 `json:"version"`
}

type PeakRequest struct {
  Name string `json:"name"`
  User string `json:"user"`
  Length int64 `json:"length"`
  Version int64 `json:"version"`
}

type WriteRequest struct {
  Name string `json:"name"`
  User string `json:"user"`
  Offset int64 `json:"offset"`
  Buffer []byte `json:"buffer"`
  Version int64 `json:"version"`
}

type AppendRequest struct {
  Name string `json:"name"`
  User string `json:"user"`
  Buffer []byte `json:"buffer"`
  Version int64 `json:"version"`
}

type CreateRequest struct {
  Name string `json:"name"`
  User string `json:"user"`
  Mode uint16 `json:"mode"`
  Version int64 `json:"version"`
}

type StatRequest struct {
  Name string `json:"name"`
  User string `json:"user"`
  Version int64 `json:"version"`
}

type UnlinkRequest struct {
  Name string `json:"name"`
  User string `json:"user"`
  Version int64 `json:"version"`
}

type RmdirRequest struct {
  Name string `json:"name"`
  User string `json:"user"`
  Version int64 `json:"version"`
}

type MkdirRequest struct {
  Name string `json:"name"`
  User string `json:"user"`
  Mode uint16 `json:"mode"`
  Version int64 `json:"version"`
}

type RenameRequest struct {
  Name string `json:"name"`
  User string `json:"user"`
  Updated string `json:"updated"`
  Version int64 `json:"version"`
}

type ReaddirRequest struct {
  Name string `json:"name"`
  User string `json:"user"`
  Version int64 `json:"version"`
}

type TruncateRequest struct {
  Name string `json:"name"`
  User string `json:"user"`
  Size int64 `json:"size"`
  Version int64 `json:"version"`
}

type UTimeNSRequest struct {
  Name string `json:"name"`
  User string `json:"user"`
  Atime int64 `json:"atime"`
  Mtime int64 `json:"mtime"`
  Version int64 `json:"version"`
}

func init() {
  store = filestore.NewFileStore("netfs", SIZE)
}

func Serialize(iface interface{}) []byte {
  buf, err := json.Marshal(iface)

  if err != nil {
    panic(err)
  }

  return buf
}

func read(request ReadRequest) []byte {
  name := request.Name
  offset := request.Offset
  length := request.Length
  version := request.Version
  user := request.User

  store.CreateUser(user)

  if store.Users[user].NameEntries[name] == nil {
    return ENOENT
  }

  file := store.Users[user].NameEntries[name]

  if file.Invalidated || file.Version < version {
    // Reach Out To Network For Valid Version
  }

  store.Read(file.Id, offset, length)
  return nil // TODO
}

func Read(w http.ResponseWriter, req * http.Request) {
  var request ReadRequest
  MessageFromStream(req.Body, &request)

  name := request.Name
  offset := request.Offset
  length := request.Length
  user := request.User

  if store.Users[user].NameEntries[name] == nil {
    w.WriteHeader(http.StatusNotFound)
    w.Write(ENOENT)
    return
  }

  file := store.Users[user].NameEntries[name]

  id := file.Id

  read := store.Read(id, offset, length)

  if offset + length > file.Size && read != nil {
    trim := file.Size - offset

    if trim < int64(len(read)) {
      trim = int64(len(read))
    }

    read = read[:trim]
  }

  if int64(len(read)) > length {
    read = read[:length]
  }

  if len(read) > 0 && int64(len(read)) > file.Size - offset {
    fmt.Printf("Slicing Curr: %d Dest: %d", len(read), file.Size - offset)
    read = read[:file.Size - offset]
  }

  fmt.Printf("Read Length: %s Position: %d, Have: %d, Buf: %d, Want: %d\n", name, offset, file.Size, len(read), length)

  w.Write(read)
}

func Write(w http.ResponseWriter, req * http.Request) {
  var request WriteRequest
  MessageFromStream(req.Body, &request)

  name := request.Name
  offset := request.Offset
  buffer := request.Buffer
  user := request.User

  store.CreateUser(user)

  file := store.Users[user].NameEntries[name]

  if file == nil {
    w.Write([]byte(ENOENT))
    return
  }

  id := file.Id

  store.Write(id, offset, buffer)

  w.Write([]byte("OK"))
}

func Create(w http.ResponseWriter, req * http.Request) {
  var request CreateRequest
  MessageFromStream(req.Body, &request)

  name := request.Name
  mode := request.Mode
  user := request.User

  store.CreateUser(user)

  id := store.CreateFile(name, user, mode, false)

  w.Write([]byte(id))
}

func Stat(w http.ResponseWriter, req * http.Request) {
  var request StatRequest
  MessageFromStream(req.Body, &request)

  store.CreateUser(request.User)

  file := store.Users[request.User].NameEntries[request.Name]

  if file == nil {
    w.Write(ENOENT)
  } else {
    buf, _ := json.Marshal(&file)
    w.Write(buf)
  }
}

func Rename(w http.ResponseWriter, req * http.Request) {
  var request RenameRequest
  MessageFromStream(req.Body, &request)

  store.CreateUser(request.User)

  file := store.Users[request.User].NameEntries[request.Name]

  if file == nil {
    w.Write(ENOENT)
  } else {
    if store.Users[request.User].NameEntries[request.Updated] != nil {
      store.Unlink(request.Updated)
    }

    store.Users[request.User].NameEntries[request.Updated] = file
    delete(store.Users[request.User].NameEntries, request.Name)
    file.Name = request.Updated
    w.Write([]byte("OK"))
    store.Save()
  }
}

func Readdir(w http.ResponseWriter, req * http.Request) {
  var request ReaddirRequest
  MessageFromStream(req.Body, &request)

  store.CreateUser(request.User)

  name := request.Name
  e, contents := store.Readdir(name, request.User)

  if e != nil {
    switch e.Code {
      case "ENOENT":
        w.Write(ENOENT)
      case "ENOTDIR":
        w.Write(ENOTDIR)
    }
    return
  }

  buf, err := json.Marshal(&contents)

  if err != nil {
    panic(err)
  }

  w.Write(buf)
}

func Unlink(w http.ResponseWriter, req * http.Request) {
  var request UnlinkRequest
  MessageFromStream(req.Body, &request)

  store.CreateUser(request.User)

  name := request.Name

  if store.Users[request.User].NameEntries[name] != nil {
    file := store.Users[request.User].NameEntries[name]
    delete(store.Users[request.User].NameEntries, name)
    store.Unlink(file.Id)
  }

  w.Write([]byte("OK"))
}

func Mkdir(w http.ResponseWriter, req * http.Request) {
  var request MkdirRequest
  MessageFromStream(req.Body, &request)
  user := request.User

  store.CreateUser(user)

  store.CreateFile(request.Name, user, request.Mode, true)

  w.Write([]byte("OK"))
}

func Rmdir(w http.ResponseWriter, req * http.Request) {
  var request RmdirRequest
  MessageFromStream(req.Body, &request)

  store.CreateUser(request.User)

  file := store.Users[request.User].NameEntries[request.Name]

  if file != nil && file.Dir {
    store.Unlink(request.Name)
  }

  w.Write([]byte("OK"))
}

func Truncate( w http.ResponseWriter, req * http.Request) {
  var request TruncateRequest
  MessageFromStream(req.Body, &request)

  fmt.Printf("Truncate Request %+v\n", request)

  store.CreateUser(request.User)

  store.Truncate(request.Name, request.User, request.Size)
  w.Write([]byte("OK"))
}

func UTimeNS(w http.ResponseWriter, req * http.Request) {
  var request UTimeNSRequest
  MessageFromStream(req.Body, &request)

  store.CreateUser(request.User)

  file := store.Users[request.User].NameEntries[request.Name]

  if file == nil {
    w.Write(ENOENT)
  } else {
    file.Accessed = request.Atime
    file.Modified = request.Mtime
    w.Write([]byte("OK"))
  }
}

func Append(w http.ResponseWriter, req * http.Request) {
  var request AppendRequest
  MessageFromStream(req.Body, &request)

  fmt.Printf("Append Request %+v\n", request)

  file := store.Users[request.User].NameEntries[request.Name]

  if file == nil {
    w.Write(ENOENT)
  } else {
    store.Write(file.Id, file.Size, request.Buffer)
    w.Write([]byte("OK"))
  }
}

func Peak(w http.ResponseWriter, req * http.Request) {
  var request PeakRequest
  MessageFromStream(req.Body, &request)

  file := store.Users[request.User].NameEntries[request.Name]

  if file == nil {
    w.Write(ENOENT)
    return
  } else {
    length := request.Length
    start := file.Size - length
    if start < 0 {
      start = 0
      length = file.Size
    }

    buf := store.Read(file.Id, start, length)
    w.Write(buf)
  }
}

func MessageFromStream(reader io.ReadCloser, iface interface{}) {
  decoder := json.NewDecoder(reader)
  err := decoder.Decode(iface)

  if err != nil {
    panic(err)
  }

  reader.Close()
}

func MessageFromBuf(buf []byte, iface interface{}) {
  err := json.Unmarshal(buf, iface)

  if err != nil {
    panic(err)
  }
}

func ws_readdir(msg * inc.INCMessage) {
  var request ReaddirRequest
  MessageFromBuf(msg.Message, &request)
  _, content := store.Readdir(request.Name, request.User)
  buf, err := json.Marshal(content)

  if err != nil {
    panic(err)
  }

  response := inc.NewINCMessage("RESPONSE", false, buf)
  response.Dest = msg.Id
  response.Rid = msg.Mid

  router.Send(string(msg.Id), response)
}

func ws_invalidate(msg * inc.INCMessage) {
  var request InvalidationRequest
  MessageFromBuf(msg.Message, &request)

  fmt.Printf("%+v\n", request)
}


func ws_unlink(msg * inc.INCMessage) {
  var request UnlinkRequest
  MessageFromBuf(msg.Message, &request)
  fmt.Printf("%+v\n", request)
}

func listen(mux * http.ServeMux) {
  fmt.Println(http.ListenAndServe(":8080", mux))
}

// func NewINCMessage(m_type string, echo bool, message []byte) *INCMessage {

func node_connected (node * inc.INCNode) {
  files := store.Serialize()
  message := inc.NewINCMessage("CONNECTLIST", false, files)
  node.Send(message)
}

func ws_connect_list(m * inc.INCMessage) {
  fmt.Println("Received Message")
}

func create_server() {
  mux := http.NewServeMux()
  mux.HandleFunc("/api/read", Read)
  mux.HandleFunc("/api/write", Write)
  mux.HandleFunc("/api/append", Append)
  mux.HandleFunc("/api/peak", Peak)
  mux.HandleFunc("/api/create", Create)
  mux.HandleFunc("/api/stat", Stat)
  mux.HandleFunc("/api/rename", Rename)
  mux.HandleFunc("/api/readdir", Readdir)
  mux.HandleFunc("/api/unlink", Unlink)
  mux.HandleFunc("/api/mkdir", Mkdir)
  mux.HandleFunc("/api/rmdir", Rmdir)
  mux.HandleFunc("/api/truncate", Truncate)
  mux.HandleFunc("/api/utimens", UTimeNS)
  mux.HandleFunc("/ws", router.HandleIncoming) // WebSocket Handler

  go listen(mux)

  readdir_chan := make(chan * inc.INCMessage)
  invalidation_chan := make(chan * inc.INCMessage)
  unlink_chan := make(chan * inc.INCMessage)
  connectlist_chan := make(chan * inc.INCMessage)


  router.On("READDIR", readdir_chan)
  router.On("INVALIDATE", invalidation_chan)
  router.On("UNLINK", unlink_chan)
  router.On("CONNECTLIST", connectlist_chan)
  router.OnConnect(node_connected)

  for {
    select {
      case msg := <- readdir_chan:
        go ws_readdir(msg)
      case msg := <- invalidation_chan:
        go ws_invalidate(msg)
      case msg := <- unlink_chan:
        go ws_unlink(msg)
      case msg := <- connectlist_chan:
        go ws_connect_list(msg)
    }
  }
}

func main() {
  fmt.Println("Starting Node")
  create_server()
  store.Close()
}
