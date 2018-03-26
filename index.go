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
  SIZE = 1024 * 1024 * 1024
)

var (
  bootstrap = []string { "192.168.30.140" }
  store * filestore.FileStore
  ENOENT = json_error("ENOENT")
  router * inc.INCRouter = inc.NewINCRouter(":8080", bootstrap)
)

func json_error(etype string) []byte {
  str := `{ "success": "false", "error": "` + etype + "\" }"
  return []byte(str)
}

type ReadRequest struct {
  Name string `json:"name"`
  Offset int64 `json:"offset"`
  Length int64 `json:"length"`
}

type WriteRequest struct {
  Name string `json:"name"`
  Offset int64 `json:"offset"`
  Buffer []byte `json:"buffer"`
}

type CreateRequest struct {
  Name string `json:"name"`
  Mode uint16 `json:"mode"`
}

type StatRequest struct {
  Name string `json:"name"`
}

type UnlinkRequest struct {
  Name string `json:"name"`
}

type RmdirRequest struct {
  Name string `json:"name"`
}

type MkdirRequest struct {
  Name string `json:"name"`
  Mode uint16 `json:"mode"`
}

type RenameRequest struct {
  Name string `json:"name"`
  Updated string `json:"updated"`
}

type ReaddirRequest struct {
  Name string `json:"name"`
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

  fmt.Printf("%+v\n", request)

  name := request.Name
  offset := request.Offset
  length := request.Length
  if store.NameEntries[name] == nil {
     w.WriteHeader(http.StatusNotFound)
     w.Write(ENOENT)
     return
  }

  id := store.NameEntries[name].Id

  read := store.Read(id, offset, length)

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

  id := store.CreateFile(name, mode, false)

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
  // msg, _ := ioutil.ReadAll(req.Body)

  // fmt.Println(string(msg))

  decoder := json.NewDecoder(req.Body)
  var request ReaddirRequest
  err := decoder.Decode(&request)

  if err != nil {
    panic(err)
  }

  name := request.Name
  contents := store.Readdir(name)

  if contents == nil {
    w.Write(ENOENT)
    return
  }

  buf, err := json.Marshal(&contents)

  if err != nil {
    panic(err)
  }

  fmt.Println(string(buf), contents)

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

  store.CreateFile(request.Name, 16877, true)

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
  mux.HandleFunc("/ws", router.HandleIncoming)
  fmt.Println(http.ListenAndServe(":8080", mux))
}

func dev_setup() {
  if store.NameEntries["/Dummy"] == nil {
    store.CreateFile("/Dummy", 33206, false)
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