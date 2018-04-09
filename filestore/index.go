package filestore

import(
  "./dumbstore"
  "fmt"
  "math"
  "os"
  "time"
  "strings"
  "io/ioutil"
  "encoding/json"
  "github.com/satori/go.uuid"
)

const SIZE = 1024 * 1024

type Error struct {
  Code string
}

func NewError(code string) * Error {
  return &Error{code}
}

var (
  ENOENT * Error = NewError("ENOENT")
  ENOTDIR * Error = NewError("ENOTDIR")
)

type UserEntries struct {
  IdEntries map[string]*MetaEntry
  NameEntries map[string]*MetaEntry
  DirEntries map[string][]*MetaEntry
}

type FileStore struct {
  Path string
  Size int64
  Store * dumbstore.FileSystem
  Entries []*MetaEntry
  IdEntries map[string]*MetaEntry
  Users map[string]*UserEntries
}

type MetaEntry struct {
  Id string `json:"id"`
  Name string `json:"name"`
  Size int64 `json:"size"`
  Mode uint16 `json:"mode"`
  Dir bool `json:"dir"`
  Invalidated bool `json:"invalidated"`
  Wanted bool `json:"wanted"`
  Parent string `json:"parent"`
  Accessed int64 `json:"accessed"`
  Created int64 `json:"created"`
  Modified int64 `json:"modified"`
  Chunks []int64 `json:"chunks"`
  Owner string
  Version int64 `json:"version"`
}

func read_file(file string) []byte {
  f, err := os.Open(file)

  if err != nil {
    return nil
  }

  defer f.Close()

  buf, err := ioutil.ReadAll(f)

  if err != nil {
    return nil
  }

  return buf
}

func write_file(file string, data []byte) {
  f, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)

  if err != nil {
    panic(err)
  }

  f.Write(data)
  f.Close()
}

func load_metadata(name string, id_entries map[string]*MetaEntry, user_entries map[string]*UserEntries) []*MetaEntry {
  buf := read_file("./" + name +  "_meta.json")
  if buf == nil {
    buf = []byte("[]")
  }

  var entries []*MetaEntry
  err := json.Unmarshal(buf, &entries)

  if err != nil {
    panic(err)
  }

  for _, entry := range entries {
    p := entry.Parent
    id := entry.Id
    name := entry.Name
    owner := entry.Owner
    id_entries[id] = entry

    if user_entries[owner] == nil {
      user_entries[owner] = &UserEntries{
        make(map[string]*MetaEntry),
        make(map[string]*MetaEntry),
        make(map[string][]*MetaEntry),
      }
    }

    user := user_entries[owner]

    user.IdEntries[id] = entry
    user.NameEntries[name] = entry
    if user.DirEntries[p] == nil {
      user.DirEntries[p] = make([]*MetaEntry, 0)
    }

    user.DirEntries[p] = append(user.DirEntries[p], entry)
  }
  return entries
}

func save_metadata(name string, entries []*MetaEntry) {
  buf, err := json.MarshalIndent(entries, "", "  ")

  if err != nil {
    panic(err)
  }

  write_file("./" + name + "_meta.json", buf)
}

func NewFileStore(name string, size int64) * FileStore {
  store := dumbstore.New(name, size)

  id_entries := make(map[string]*MetaEntry)
  user_entries := make(map[string]*UserEntries)

  entries := load_metadata(name, id_entries, user_entries)

  file_store := &FileStore{
    name,
    size,
    store,
    entries,
    id_entries,
    user_entries,
  }

  for name, user := range user_entries {
    if user.NameEntries["/"] == nil {
      file_store.CreateFile("/", name, 16822, true)
    }
  }

  return file_store
}

func (this * FileStore) Close() {
  this.Save()
}

func (this * FileStore) Serialize() []byte {
  buf, err := json.Marshal(this.Entries)
  if err != nil {
    panic(err)
  }

  return buf
}

func (this *FileStore) CreateFile(name string, user string, mode uint16, dir bool) string {

  if this.Users[user].NameEntries[name] != nil {
    return this.Users[user].NameEntries[name].Id
  }

  f_type := "File"

  if dir {
    f_type = "Dir"
  }

  fmt.Printf("Creating %s (%s)\n", f_type, name)

  // fmt.Println(this.NameEntries)

  if name == "ROOT" && this.IdEntries["ROOT"] != nil {
    return "ROOT"
  }

  id := uuid.Must(uuid.NewV4()).String()

  if name == "ROOT" || name == "/" {
    // fmt.Println("Updating Root ID")
    id = "ROOT"
    name = "ROOT"
  }

  t := time.Now().UnixNano()

  var parent string

  index := strings.LastIndex(name, "/")

  if index > -1 {
    parent = name[:index]

    if parent == "" {
      parent = "/"
    }

    if this.Users[user].NameEntries[parent] == nil && name != "/" {
      this.CreateFile(parent, user, 16877, true)
    }

    if parent == "/" {
      parent = "ROOT"
    } else {
      parent = this.Users[user].NameEntries[parent].Id
    }
  } else {
    parent = "ROOT"
  }

  if name == "ROOT" {
    name = "/"
  }

  entry := &MetaEntry {
    id,
    name,
    0,
    mode,
    dir,
    false,
    true,
    parent,
    t,
    t,
    t,
    make([]int64, 0),
    user,
    0,
  }
  this.Entries = append(this.Entries, entry)
  this.IdEntries[id] = entry
  this.Users[user].NameEntries[name] = entry
  this.Users[user].IdEntries[id] = entry
  this.Users[user].DirEntries[parent] = append(this.Users[user].DirEntries[parent], entry)
  this.Save()
  return id
}

func compute_data(position int64, buf []byte) (int64, int64, []byte) {
  l := int64(len(buf))
  chunk := int64(float64(position) / float64(dumbstore.WRITE_SIZE))
  offset := position % dumbstore.WRITE_SIZE
  computed_len := dumbstore.WRITE_SIZE - offset

  if l < computed_len {
    computed_len = l
  }

  computed_buf := buf[:computed_len]
  return chunk, offset, computed_buf
}
                                                        /* chunk, start, end */
func compute_next_read(start int64, end int64) (int64, int64, int64) {
  chunk := int64(float64(start) / float64(dumbstore.WRITE_SIZE))

  remaining := dumbstore.WRITE_SIZE - (start % dumbstore.WRITE_SIZE)

  if remaining == 0 {
    remaining = dumbstore.WRITE_SIZE
  }

  start_pos := start % dumbstore.WRITE_SIZE
  end_pos := start_pos + remaining

  if end_pos > end {
    end_pos = end
  }

  return chunk, start_pos, end_pos
}

func (this * FileStore) Write(id string, position int64, data []byte) {
  file := this.IdEntries[id]

  if file == nil {
    return
  }

  file.Accessed = time.Now().UnixNano()
  file.Modified = time.Now().UnixNano()

  start := int64(float64(position) / float64(dumbstore.WRITE_SIZE))
  end := int64(float64(position + int64(len(data))) / float64(dumbstore.WRITE_SIZE))

  for i := int64(len(file.Chunks)); i <= end; i++ {
    next := this.Store.GetNextAvailable()

    if next == -1 {
      panic(dumbstore.E_FULL)
    }

    this.Store.Reserve(next)
    file.Chunks = append(file.Chunks, next)
  }

  offset := position % dumbstore.WRITE_SIZE
  overflow := position + int64(len(data)) % dumbstore.WRITE_SIZE

  written := int64(0)
  chunk, offset, computed_buf := compute_data(position, data)
  chunk = file.Chunks[chunk]
  // fmt.Println("Writing to", chunk, offset)
  this.Store.OverwriteOffset(chunk, offset, computed_buf)
  written += int64(len(computed_buf))

  for i := start + 1; i < end; i++ {
    chunk, offset, computed_buf := compute_data(position + written, data[written:])
    chunk = file.Chunks[chunk]
    this.Store.OverwriteOffset(chunk, offset, computed_buf)
    written += int64(len(computed_buf))
  }

  if overflow > 0 {
    chunk, offset, computed_buf := compute_data(position + written, data[written:])


    if chunk >= int64(len(file.Chunks)) {
      next := this.Store.GetNextAvailable()

      if next == -1 {
        panic(dumbstore.E_FULL)
      }

      file.Chunks = append(file.Chunks, next)
    }

    chunk = file.Chunks[chunk]
    this.Store.OverwriteOffset(chunk, offset, computed_buf)
    written += int64(len(computed_buf))
  }

  size := position + written

  if size >= file.Size {
    file.Size = size
  }
  this.Save()
  // fmt.Printf("Wrote %d Bytes\n", written)
}

func (this * FileStore) Read(id string, position int64, length int64) []byte {
  file := this.IdEntries[id]

  if file == nil || position > file.Size {
    return make([]byte, 0)
  }

  file_start := position
  file_end := position + length

  if file_end > file.Size {
    file_end = file.Size
  }

  file.Accessed = time.Now().UnixNano()

  buf := make([]byte, length)
  read := int64(0)

  for i := int64(0); file_start + read < file_end && i < int64(len(file.Chunks)); i++ {
    chunk, start, end := compute_next_read(file_start + read, file_end)
    var chunk_data []byte

    if chunk >= int64(len(file.Chunks)) {
      return buf[:read]
    } else {
      mapped_chunk := file.Chunks[chunk]
      chunk_data = this.Store.ReadChunk(mapped_chunk)
    }

    read_buf := chunk_data[start:end]
    copy(buf[read:], read_buf)
    read += end - start
  }
  this.Save()

  slice_size := read

  if length < slice_size {
    slice_size = length
  }

  if slice_size > int64(len(buf)) {
    slice_size = int64(len(buf))
  }

  return buf[:slice_size]
}

func (this * FileStore) Unlink(id string) {
  if this.IdEntries[id] == nil {
    return
  }

  file := this.IdEntries[id]

  for _, chunk := range file.Chunks {
    this.Store.Free(chunk)
  }

  delete(this.IdEntries, id)
  this.Save()
}

func (this * FileStore) Truncate(name string, user string, size int64) {
  if this.Users[user].NameEntries[name] == nil {
    return
  }

  file := this.Users[user].NameEntries[name]

  // Size in bytes of the blocks needed
  block_size := int64(math.Ceil(float64(size) / float64(dumbstore.WRITE_SIZE)) * float64(dumbstore.WRITE_SIZE))
  i := int64(0)
  for ; (i * dumbstore.WRITE_SIZE) < block_size; i++ {
    if i < int64(len(file.Chunks)) {
      continue
    }

    next := this.Store.GetNextAvailable()

    if next == -1 {
      panic(dumbstore.E_FULL)
    }

    this.Store.Reserve(next)
  }

  if i < int64(len(file.Chunks)) {
    for j := i; j < int64(len(file.Chunks)); j++ {
      this.Store.Free(file.Chunks[i])
    }

    file.Chunks = file.Chunks[:i]
  }

  file.Size = size
  this.Save()
}

func (this * FileStore) Readdir(name string, user string) (*Error, []string) {
  file := this.Users[user].NameEntries[name]

  // fmt.Println(name, file.Id, file.Parent, file.Dir, file)

  if file == nil {
    fmt.Println(file)
    return ENOENT, nil
  } else if !file.Dir {
    fmt.Printf("%s, %s, %+v\n", name, user, file)
    return ENOTDIR, nil
  }

  pid := file.Id

  contents := make([]string, 0)

  for _, entry := range this.Entries {
    if entry.Parent == pid && entry.Id != "ROOT" {
      contents = append(contents, entry.Name)
    }
  }

  for i, entry := range contents {
    index := strings.LastIndex(entry, "/")
    if index > -1 {
      contents[i] = entry[index + 1:]
    }
  }

  return nil, contents
}

func (this * FileStore) Save() {
  save_metadata(this.Path, this.Entries)
}

func (this * FileStore) CreateUser(user string) {
  if this.Users[user] == nil {
    this.Users[user] = &UserEntries{
      make(map[string]*MetaEntry),
      make(map[string]*MetaEntry),
      make(map[string][]*MetaEntry),
    }
  }

  this.CreateFile("/", user, 16822, true)
}

func main() {
  // fmt.Println("Starting File Store")
  file_store := NewFileStore("data", SIZE)

  var id string

  if file_store.Users["*"].NameEntries["Dummy"] != nil {
    id = file_store.Users["*"].NameEntries["Dummy"].Id
  } else {
      id = file_store.CreateFile("Dummy", "*", 33206, true)
  }

  file := file_store.IdEntries[id]

  for i := 0; i < 1000; i++ {
    file_store.Write(id, file.Size, []byte("Hello World\nTesting\n"))
  }

  file_store.Close()
}
