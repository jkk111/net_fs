package main

import (
  "./inc"
  "./filestore"
  "fmt"
  "encoding/json"
  "net/http"
  "io/ioutil"
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
  str := `{ "success": "false", "error": "` + etype + "\" }"
  return []byte(str)
}

type ReadRequest struct {
  Name string `json:"name"`
  User string `json:"user"`
  Offset int64 `json:"offset"`
  Length int64 `json:"length"`
}

type WriteRequest struct {
  Name string `json:"name"`
  User string `json:"user"`
  Offset int64 `json:"offset"`
  Buffer []byte `json:"buffer"`
}

type CreateRequest struct {
  Name string `json:"name"`
  User string `json:"user"`
  Mode uint16 `json:"mode"`
}

type StatRequest struct {
  Name string `json:"name"`
  User string `json:"user"`
}

type UnlinkRequest struct {
  Name string `json:"name"`
  User string `json:"user"`
}

type RmdirRequest struct {
  Name string `json:"name"`
  User string `json:"user"`
}

type MkdirRequest struct {
  Name string `json:"name"`
  User string `json:"user"`
  Mode uint16 `json:"mode"`
}

type RenameRequest struct {
  Name string `json:"name"`
  User string `json:"user"`
  Updated string `json:"updated"`
}

type ReaddirRequest struct {
  Name string `json:"name"`
  User string `json:"user"`
}

type TruncateRequest struct {
  Name string `json:"name"`
  User string `json:"user"`
  Size int64 `json:"size"`
}

type UTimeNSRequest struct {
  Name string `json:"name"`
  User string `json:"user"`
  Atime int64 `json:"atime"`
  Mtime int64 `json:"mtime"`
}

func init() {
  store = filestore.NewFileStore("netfs", SIZE)
}

func Read(w http.ResponseWriter, req * http.Request) {
  decoder := json.NewDecoder(req.Body)
  var request ReadRequest
  err := decoder.Decode(&request)

  if err != nil {
    panic(err)
  }

  defer req.Body.Close()

  name := request.Name
  offset := request.Offset
  length := request.Length
  if store.NameEntries[name] == nil {
     w.WriteHeader(http.StatusNotFound)
     w.Write(ENOENT)
     return
  }

  file := store.NameEntries[name]

  id := file.Id

  read := store.Read(id, offset, length)

  if offset + length > file.Size {
    read = read[:file.Size - offset]
  }

  w.Write(read)
}

func Write(w http.ResponseWriter, req * http.Request) {
  decoder := json.NewDecoder(req.Body)
  var request WriteRequest
  err := decoder.Decode(&request)

  if err != nil {
    panic(err)
  }

  // fmt.Printf("WRITE REQUEST: %+v\n", request)

  defer req.Body.Close()

  name := request.Name
  offset := request.Offset
  buffer := request.Buffer

  id := store.NameEntries[name].Id

  store.Write(id, offset, buffer)

  w.Write([]byte("OK"))
}

func Create(w http.ResponseWriter, req * http.Request) {
  decoder := json.NewDecoder(req.Body)
  var request CreateRequest
  err := decoder.Decode(&request)

  if err != nil {
    panic(err)
  }

  defer req.Body.Close()

  name := request.Name
  mode := request.Mode
  user := request.User

  id := store.CreateFile(name, user, mode, false)

  w.Write([]byte(id))
}

func Stat(w http.ResponseWriter, req * http.Request) {
  // decoder := json.NewDecoder(req.Body)

  buf, err := ioutil.ReadAll(req.Body)
  var request StatRequest
  // err := decoder.Decode(&request)

  err2 := json.Unmarshal(buf, &request)

  // fmt.Printf("%+v\n", request, err2)

  if err != nil {
    panic(err)
  } else if err2 != nil {
    panic(err2)
  }

  defer req.Body.Close()

  file := store.NameEntries[request.Name]

  if file == nil {
    w.Write(ENOENT)
  } else {
    // fmt.Printf("%+v\n", file)
    buf, _ := json.Marshal(&file)
    w.Write(buf)
  }
}

func Rename(w http.ResponseWriter, req * http.Request) {
  decoder := json.NewDecoder(req.Body)
  var request RenameRequest
  err := decoder.Decode(&request)

  if err != nil {
    panic(err)
  }

  defer req.Body.Close()

  file := store.NameEntries[request.Name]

  if file == nil {
    w.Write(ENOENT)
  } else {
    if store.NameEntries[request.Updated] != nil {
      store.Unlink(request.Updated)
    }

    store.NameEntries[request.Updated] = file
    delete(store.NameEntries, request.Name)
    file.Name = request.Updated
    w.Write([]byte("OK"))
    store.Save()
  }
}

func Readdir(w http.ResponseWriter, req * http.Request) {
  decoder := json.NewDecoder(req.Body)
  var request ReaddirRequest
  err := decoder.Decode(&request)

  if err != nil {
    panic(err)
  }

  name := request.Name
  e, contents := store.Readdir(name)

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
  decoder := json.NewDecoder(req.Body)
  var request UnlinkRequest
  err := decoder.Decode(&request)

  if err != nil {
    panic(err)
  }

  name := request.Name

  if store.NameEntries[name] != nil {
    store.Unlink(name)
  }

  w.Write([]byte("OK"))
}

func Mkdir(w http.ResponseWriter, req * http.Request) {
  decoder := json.NewDecoder(req.Body)
  var request MkdirRequest
  err := decoder.Decode(&request)

  if err != nil {
    panic(err)
  }

  user := request.User

  store.CreateFile(request.Name, user, request.Mode, true)

  w.Write([]byte("OK"))
}

func Rmdir(w http.ResponseWriter, req * http.Request) {
  decoder := json.NewDecoder(req.Body)
  var request RmdirRequest
  err := decoder.Decode(&request)

  if err != nil {
    panic(err)
  }

  file := store.NameEntries[request.Name]

  if file != nil && file.Dir {
    store.Unlink(request.Name)
  }

  w.Write([]byte("OK"))
}

func Truncate( w http.ResponseWriter, req * http.Request) {
  decoder := json.NewDecoder(req.Body)
  var request TruncateRequest
  err := decoder.Decode(&request)

  if err != nil {
    panic(err)
  }


  store.Truncate(request.Name, request.Size)



  w.Write([]byte("OK"))
}

func UTimeNS(w http.ResponseWriter, req * http.Request) {
  decoder := json.NewDecoder(req.Body)
  var request UTimeNSRequest
  err := decoder.Decode(&request)

  if err != nil {
    panic(err)
  }

  file := store.NameEntries[request.Name]
  file.Accessed = request.Atime
  file.Modified = request.Mtime

  w.Write([]byte("OK"))
}

func ws_readdir(msg * inc.INCMessage) {
  _, content := store.Readdir("")
  buf, err := json.Marshal(content)

  if err != nil {
    panic(err)
  }

  response := inc.NewINCMessage("RESPONSE", false, buf)
  response.Dest = msg.Id
  response.Rid = msg.Mid

  router.Send(string(msg.Id), response)
}

func ws_invalidate(msg * inc.INCMessage) {}

func create_server() {
  mux := http.NewServeMux()
  mux.HandleFunc("/api/read", Read)
  mux.HandleFunc("/api/write", Write)
  mux.HandleFunc("/api/create", Create)
  mux.HandleFunc("/api/stat", Stat)
  mux.HandleFunc("/api/rename", Rename)
  mux.HandleFunc("/api/readdir", Readdir)
  mux.HandleFunc("/api/unlink", Unlink)
  mux.HandleFunc("/api/mkdir", Mkdir)
  mux.HandleFunc("/api/rmdir", Rmdir)
  mux.HandleFunc("/api/truncate", Truncate)
  mux.HandleFunc("/api/utimens", UTimeNS)
  mux.HandleFunc("/ws", router.HandleIncoming)
  fmt.Println(http.ListenAndServe(":8080", mux))

  readdir_chan := make(chan * inc.INCMessage)
  invalidation_chan := make(chan * inc.INCMessage)

  router.On("READDIR", readdir_chan)
  router.On("INVALIDATE", invalidation_chan)

  for {
    select {
      case msg := <- readdir_chan:
        go ws_readdir(msg)
      case msg := <- invalidation_chan:
        go ws_invalidate(msg)
    }
  }
}

func dev_setup() {
  if store.NameEntries["/Dummy"] == nil {
    store.CreateFile("/Dummy", "*", 33206, false)
  }
  file := store.NameEntries["/Dummy"]
  id := file.Id
  store.Write(id, 0, []byte("Hello World"))
  fmt.Println("Dummy File ID:", id)
}

func main() {
  dev_setup()
  fmt.Println("Starting Node")
  create_server()
  store.Close()
}
