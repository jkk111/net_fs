package main

import (
  "./inc"
  "./cache"
  "./filestore"
  "fmt"
  "encoding/json"
  "net/http"
  "io"
  "math"
  // "io/ioutil"
)

const (
  DEFAULT_PORT = ":8090"
  SIZE = 5 * 1024 * 1024 * 1024
)

// 192.168.30.140:8080/ws

var (
  bootstrap = []string { "john-kevin.me:8090" }
  store * filestore.FileStore
  ENOENT = json_error("ENOENT")
  ENOTDIR = json_error("ENOTDIR")
  router * inc.INCRouter = inc.NewINCRouter(":8080")
  hot_cache = cache.NewCache(1024)
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

func read(id string, offset, length int64) []byte {
  file := store.Entries.IdEntries[id]

  if file == nil {
    return ENOENT
  }

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

  return read
}

func write(id string, offset int64, buf []byte) {
  store.Write(id, offset, buf)
}

func write_remote(remote string, write_request WriteRequest) {
  request := Serialize(write_request)
  message := inc.NewINCMessage("WRITE", false, request)
  router.Send(remote, message)
}

func read_remote(remote string, read_request ReadRequest) []byte {
  file := store.LatestName(read_request.User, read_request.Name)

  ws := float64(filestore.WRITE_SIZE)

  start := read_request.Offset
  end := start + read_request.Length

  start_f := float64(start)
  end_f := float64(end)

  start_off := read_request.Offset % filestore.WRITE_SIZE

  start_chunk := int64(math.Floor(start_f / ws))
  end_chunk := int64(math.Ceil(end_f / ws))

  buf := make([]byte, read_request.Length)

  written := 0

  for i := start_chunk; i <= end_chunk; i++ {
    fmt.Printf("Getting Chunks %d => %d => %d\n", start_chunk, i, end_chunk)
    is_eof := false
    data := hot_cache.Get(file.Id, i)

    if data == nil {
      request := Serialize(&ReadRequest{
        User: read_request.User,
        Name: read_request.Name,
        Offset: start_chunk * filestore.WRITE_SIZE,
        Length: filestore.WRITE_SIZE,
      })

      message := inc.NewINCMessage("READ", false, request)

      resp_ch := make(chan * inc.INCMessage)
      router.Await(string(message.Mid), resp_ch)
      router.Send(remote, message)
      fmt.Printf("Sent Read Request to %s (%s)\n", remote, string(message.Mid))
      resp := <- resp_ch

      fmt.Println("Received Read Response", len(resp.Message))
      fmt.Println("Adding To Cache")
      hot_cache.Add(file.Id, i, resp.Message)

      data = resp.Message

      if int64(len(resp.Message)) < filestore.WRITE_SIZE {
        is_eof = true
      }
    }

    if i == start {
      data = data[start_off:]
    }

    written += copy(buf[written:], data)

    if is_eof {
      break
    }
  }

  return buf

  // request := Serialize(read_request)
  // message := inc.NewINCMessage("READ", false, request)

  // resp_ch := make(chan * inc.INCMessage)
  // router.Await(string(message.Mid), resp_ch)
  // router.Send(remote, message)
  // fmt.Println("Sent Read Request")
  // resp := <- resp_ch
  // fmt.Println("Received Read Response", len(resp.Message))

  // return resp.Message
}

func remote_peak(remote string, peak_request PeakRequest) []byte {
  request := Serialize(peak_request)
  message := inc.NewINCMessage("PEAK", false, request)

  resp_ch := make(chan * inc.INCMessage)
  router.Await(string(message.Mid), resp_ch)
  router.Send(remote, message)

  resp := <- resp_ch

  return resp.Message
}

func remote_append(remote string, append_request AppendRequest) {
  request := Serialize(append_request)
  message := inc.NewINCMessage("APPEND", false, request)
  router.Send(remote, message)
}

func Read(w http.ResponseWriter, req * http.Request) {
  var request ReadRequest
  MessageFromStream(req.Body, &request)

  name := request.Name
  user := request.User
  file := store.LatestName(user, name)
  store.CreateUser(user)

  if file == nil {
    fmt.Println("File Not Found", name)
    w.WriteHeader(http.StatusNotFound)
    w.Write(ENOENT)
    return
  }

  offset := request.Offset
  length := request.Length
  var data []byte

  fmt.Printf("%+v\n", store.Entries)

  if file.Remote {
    data = read_remote(file.RemoteHost, request)
  } else {
    data = read(file.Id, offset, length)
  }

  fmt.Printf("Read Length: %s Position: %d, Have: %d, Buf: %d, Want: %d\n", name, offset, file.Size, len(data), length)
  w.Write(data)
  fmt.Println("Wrote Response")
}

func Write(w http.ResponseWriter, req * http.Request) {
  var request WriteRequest
  MessageFromStream(req.Body, &request)

  name := request.Name
  offset := request.Offset
  buffer := request.Buffer
  user := request.User

  store.CreateUser(user)

  file := store.LatestName(user, name)

  if file == nil {
    w.Write([]byte(ENOENT))
    return
  }

  id := file.Id
  if file.Remote {
    write_remote(file.RemoteHost, request)
  } else {
    write(id, offset, buffer)
  }

  w.Write([]byte("OK"))
}

func unlink(user, name string) {
  file := store.LatestName(user, name)
  store.Unlink(file.Id)
}

func remote_unlink(remote string, unlink_request UnlinkRequest) {
  request := Serialize(unlink_request)
  message := inc.NewINCMessage("UNLINK", true, request)

  router.Send(remote, message)
}

func Create(w http.ResponseWriter, req * http.Request) {
  var request CreateRequest
  MessageFromStream(req.Body, &request)

  name := request.Name
  mode := request.Mode
  user := request.User

  store.CreateUser(user)

  id := store.CreateFile(user, name, mode, false)

  w.Write([]byte(id))
}

func ws_create(m * inc.INCMessage) {
  // var request
}

func ws_mkdir(m * inc.INCMessage) {

}

func ws_rmdir(m * inc.INCMessage) {
  var request RmdirRequest
  MessageFromBuf(m.Message, &request)
  fmt.Printf("TODO WS_rmdir%+v\n", request)
}

func ws_truncate(m * inc.INCMessage) {
  var request TruncateRequest
  MessageFromBuf(m.Message, &request)
}

func Stat(w http.ResponseWriter, req * http.Request) {
  var request StatRequest
  MessageFromStream(req.Body, &request)

  fmt.Printf("%+v\n", request)

  store.CreateUser(request.User)

  user := request.User
  name := request.Name

  file := store.LatestName(user, name)

  if file == nil {
    fmt.Printf("File Is Nil %s\n", name)
    w.Write(ENOENT)
  } else {
    buf, _ := json.Marshal(&file)

    fmt.Println("Stated")

    w.Write(buf)
  }
}

func remote_rename(remote string, rename_request RenameRequest) {
  request := Serialize(rename_request)
  message := inc.NewINCMessage("RENAME", false, request)
  router.Send(remote, message)
}

func rename(user string, name string, updated string) {
  file := store.LatestName(user, name)
  existing := store.LatestName(user, updated)
  if existing != nil {
    store.Unlink(existing.Id)
  }

  store.Entries.Remove(file.Id)
  file.Name = updated
  store.Entries.Add(file)
}

func Rename(w http.ResponseWriter, req * http.Request) {
  var request RenameRequest
  MessageFromStream(req.Body, &request)

  store.CreateUser(request.User)

  user := request.User
  name := request.Name

  file := store.LatestName(user, name)

  if file == nil {
    w.Write(ENOENT)
  }

  if file.Remote {
    remote_rename(file.RemoteHost, request)
  } else {
    rename(user, name, request.Updated)
    w.Write([]byte("OK"))
    store.Save()
  }
}

func Readdir(w http.ResponseWriter, req * http.Request) {
  var request ReaddirRequest
  MessageFromStream(req.Body, &request)

  store.CreateUser(request.User)

  name := request.Name
  e, contents := store.Readdir(request.User, name)

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

  user := request.User
  name := request.Name

  file := store.LatestName(user, name)

  if file.Remote {
    remote_unlink(file.RemoteHost, request)
  } else {
    unlink(user, name)
  }

  w.Write([]byte("OK"))
}

func Mkdir(w http.ResponseWriter, req * http.Request) {
  var request MkdirRequest
  MessageFromStream(req.Body, &request)
  user := request.User

  store.CreateUser(user)

  store.CreateFile(user, request.Name, request.Mode, true)

  w.Write([]byte("OK"))
}

func Rmdir(w http.ResponseWriter, req * http.Request) {
  var request RmdirRequest
  MessageFromStream(req.Body, &request)

  store.CreateUser(request.User)

  user := request.User
  name := request.Name

  file := store.LatestName(user, name)

  if file != nil && file.Dir {
    if file.Remote {
      remote_unlink(file.RemoteHost, UnlinkRequest(request))
    } else {
      store.Unlink(request.Name)
    }
  }

  w.Write([]byte("OK"))
}

func remote_truncate(remote string, truncate_request TruncateRequest) {
  request := Serialize(truncate_request)
  message := inc.NewINCMessage("TRUNCATE", false, request)
  router.Send(remote, message)
}

func truncate(user, name string, size int64) {
  store.CreateUser(user)
  store.Truncate(user, name, size)
}

func Truncate( w http.ResponseWriter, req * http.Request) {
  var request TruncateRequest
  MessageFromStream(req.Body, &request)
  fmt.Printf("Truncate Request %+v\n", request)
  store.CreateUser(request.User)

  user := request.User
  name := request.Name

  file := store.LatestName(user, name)

  if file.Remote {
    remote_truncate(file.RemoteHost, request)
  } else {
    truncate(user, name, request.Size)
  }

  w.Write([]byte("OK"))
}

func remote_utimens(remote string, utimens_request UTimeNSRequest) {
  request := Serialize(utimens_request)
  message := inc.NewINCMessage("UTIMENS", false, request)
  router.Send(remote, message)
}

func utimens(id string, accessed, modified int64) {
  file := store.Entries.IdEntries[id]
  file.Accessed = accessed
  file.Modified = modified
}

func UTimeNS(w http.ResponseWriter, req * http.Request) {
  var request UTimeNSRequest
  MessageFromStream(req.Body, &request)

  store.CreateUser(request.User)

  user := request.User
  name := request.Name

  file := store.LatestName(user, name)

  if file == nil {
    w.Write(ENOENT)
  }

  if file.Remote {
    remote_utimens(file.RemoteHost, request)
  } else {
    utimens(file.Id, request.Atime, request.Mtime)
    w.Write([]byte("OK"))
  }
}

func ws_utimens(m * inc.INCMessage) {
  var request UTimeNSRequest
  MessageFromBuf(m.Message, &request)

  user := request.User
  name := request.Name

  file := store.LatestName(user, name)

  if file.Remote {
    remote_utimens(file.RemoteHost, request)
  } else {
    utimens(file.Id, request.Atime, request.Mtime)
  }
}

func Append(w http.ResponseWriter, req * http.Request) {
  var request AppendRequest
  MessageFromStream(req.Body, &request)

  fmt.Printf("Append Request %+v\n", request)


  user := request.User
  name := request.Name

  file := store.LatestName(user, name)

  if file == nil {
    w.Write(ENOENT)
  }


  if file.Remote {
    remote_append(file.RemoteHost, request)
  } else {
    store.Write(file.Id, file.Size, request.Buffer)
    w.Write([]byte("OK"))
  }
}

func peak(id string, length int64) []byte {
  file := store.Entries.IdEntries[id]

  start := file.Size - length
  if start < 0 {
    start = 0
    length = file.Size
  }

  return store.Read(file.Id, start, length)
}

func Peak(w http.ResponseWriter, req * http.Request) {
  var request PeakRequest
  MessageFromStream(req.Body, &request)


  user := request.User
  name := request.Name

  file := store.LatestName(user, name)

  if file == nil {
    w.Write(ENOENT)
    return
  }

  var data []byte

  if file.Remote {
    data = remote_peak(file.RemoteHost, request)
  } else {
    data = peak(file.Id, request.Length)
  }
  w.Write(data)
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

func listen(mux * http.ServeMux) {
  fmt.Println(http.ListenAndServe(":8090", mux))
}

func node_connected (node * inc.INCNode) {
  fmt.Println("Node Connected, Sending File List")
  files := store.Serialize()
  message := inc.NewINCMessage("CONNECTLIST", true, files)
  router.Send(string(node.Id), message)
}

func ws_invalidate(msg * inc.INCMessage) {
  var request InvalidationRequest
  MessageFromBuf(msg.Message, &request)
  fmt.Printf("TODO WS_INVALIDATE%+v\n", request)
}


func ws_unlink(msg * inc.INCMessage) {
  var request UnlinkRequest
  MessageFromBuf(msg.Message, &request)
  fmt.Printf("TODO WS_UNLINK%+v\n", request)
}

func ws_connect_list(m * inc.INCMessage) {
  fmt.Printf("%s %s\n", string(m.Id), string(m.Message))


  store.ParseRemote(string(m.Id), m.Message)
}

func ws_read(m * inc.INCMessage) {
  var request ReadRequest
  MessageFromBuf(m.Message, &request)

  user := request.User
  name := request.Name

  file := store.LatestName(user, name)

  if file == nil {
    fmt.Printf("%+v\n", request)
    panic("This Shouldn't Happen")
  }

  offset := request.Offset
  length := request.Length

  var data []byte

  if file.Remote {
    fmt.Println("WS PROXY READ", file)
    data = read_remote(file.RemoteHost, request)
  } else {
    data = read(file.Id, offset, length)
  }

  msg := inc.NewINCMessage("READ_RESPONSE", false, data)
  msg.Rid = m.Mid

  router.Send(string(m.Id), msg)
}

func ws_write(m * inc.INCMessage) {
  var request WriteRequest
  MessageFromBuf(m.Message, &request)

  user := request.User
  name := request.Name

  file := store.LatestName(user, name)

  if file == nil {
    panic("Broke")
  }

  offset := request.Offset
  buffer := request.Buffer

  if file.Remote {
    write_remote(file.RemoteHost, request)
  } else {
    write(file.Id, offset, buffer)
  }
}

func ws_append(m * inc.INCMessage) {
  var request AppendRequest
  MessageFromBuf(m.Message, &request)

  user := request.User
  name := request.Name

  file := store.LatestName(user, name)

  if file.Remote {
    remote_append(file.RemoteHost, request)
  } else {
    buffer := request.Buffer
    store.Write(file.Id, file.Size, buffer)
  }
}

func ws_peak(m * inc.INCMessage) {
  var request PeakRequest
  MessageFromBuf(m.Message, &request)

  user := request.User
  name := request.Name
  length := request.Length

  file := store.LatestName(user, name)

  var data []byte

  if file.Remote {
    data = remote_peak(file.RemoteHost, request)
  } else {
    data = peak(file.Id, length)
  }

  msg := inc.NewINCMessage("READ_RESPONSE", false, data)
  msg.Rid = m.Mid

  router.Send(string(m.Id), msg)
}

func ws_rename(m * inc.INCMessage) {
  var request RenameRequest
  MessageFromBuf(m.Message, &request)

  user := request.User
  name := request.Name

  file := store.LatestName(user, name)

  if file.Remote {
    remote_rename(file.RemoteHost, request)
  } else {
    rename(user, name, request.Updated)
  }
}

func create_server() {
  mux := http.NewServeMux()
  mux.HandleFunc("/api/read", Read) // Exposed On Socket
  mux.HandleFunc("/api/write", Write) // Exposed On Socket
  mux.HandleFunc("/api/append", Append) // Exposed On Socket
  mux.HandleFunc("/api/peak", Peak) // Exposed On Socket
  mux.HandleFunc("/api/create", Create) // Exposed On Socket
  mux.HandleFunc("/api/stat", Stat) // Exposed On Socket
  mux.HandleFunc("/api/rename", Rename) // Exposed On Socket
  mux.HandleFunc("/api/readdir", Readdir)
  mux.HandleFunc("/api/unlink", Unlink) // Exposed On Socket
  mux.HandleFunc("/api/mkdir", Mkdir) // Exposed On Socket
  mux.HandleFunc("/api/rmdir", Rmdir) // Exposed On Socket
  mux.HandleFunc("/api/truncate", Truncate) // Exposed On Socket
  mux.HandleFunc("/api/utimens", UTimeNS) // Exposed On Socket
  mux.HandleFunc("/ws", router.HandleIncoming) // WebSocket Handler

  go listen(mux)

  write_chan := make(chan * inc.INCMessage)
  read_chan := make(chan * inc.INCMessage)
  invalidation_chan := make(chan * inc.INCMessage)
  unlink_chan := make(chan * inc.INCMessage)
  connectlist_chan := make(chan * inc.INCMessage)

  append_chan := make(chan * inc.INCMessage)
  peak_chan := make(chan * inc.INCMessage)
  rename_chan := make(chan * inc.INCMessage)
  create_chan := make(chan * inc.INCMessage)

  mkdir_chan := make(chan * inc.INCMessage)
  rmdir_chan := make(chan * inc.INCMessage)
  truncate_chan := make(chan * inc.INCMessage)
  utimens_chan := make(chan * inc.INCMessage)

  // Can Request Across Other Nodes
  router.On("READ", read_chan)
  router.On("WRITE", write_chan)
  router.On("UNLINK", unlink_chan)

  router.On("APPEND", append_chan)
  router.On("PEAK", peak_chan)

  router.On("RENAME", rename_chan)
  router.On("CREATE", create_chan)
  router.On("MKDIR", mkdir_chan)
  router.On("RMDIR", rmdir_chan)

  router.On("TRUNCATE", truncate_chan)
  router.On("UTIMENS", utimens_chan)


  // Invalidate cache
  router.On("INVALIDATE", invalidation_chan)

  //Init
  router.On("CONNECTLIST", connectlist_chan)
  router.OnConnect(node_connected)

  router.BootstrapNodes(bootstrap)

  go router.HandleMessages()

  for {
    select {
      case msg := <- invalidation_chan:
        go ws_invalidate(msg)
      case msg := <- unlink_chan:
        go ws_unlink(msg)
      case msg := <- connectlist_chan:
        go ws_connect_list(msg)
      case msg := <- read_chan:
        go ws_read(msg)
      case msg := <- write_chan:
        go ws_write(msg)
      case msg := <- append_chan:
        go ws_append(msg)
      case msg := <- peak_chan:
        go ws_peak(msg)
      case msg := <- rename_chan:
        go ws_rename(msg)
      case msg := <- create_chan:
        go ws_create(msg)
      case msg := <- mkdir_chan:
        go ws_mkdir(msg)
      case msg := <- rmdir_chan:
        go ws_rmdir(msg)
      case msg := <- truncate_chan:
        go ws_truncate(msg)
      case msg := <- utimens_chan:
        go ws_utimens(msg)
    }
  }
}

func main() {
  fmt.Println("Starting Node")
  create_server()
  store.Close()
}
