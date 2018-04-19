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
const WRITE_SIZE = dumbstore.WRITE_SIZE

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

type EntrySet struct {
  Entries []*MetaEntry
  IsRemote bool
  RemoteId string
  IdEntries map[string]*MetaEntry
  Users map[string]*UserEntries
}

func (this * EntrySet) Add(entry * MetaEntry) {
  p := entry.Parent
  name := entry.Name
  id := entry.Id
  user := entry.Owner

  if this.IsRemote {
    entry.RemoteHost = this.RemoteId
    entry.Remote = true
  }

  if entry.Dir {
    this.Users[user].DirEntries[id] = make([]*MetaEntry, 0)
  }

  this.Entries = append(this.Entries, entry)
  this.IdEntries[id] = entry
  this.Users[user].IdEntries[id] = entry
  this.Users[user].NameEntries[name] = entry
  this.Users[user].DirEntries[p] = append(this.Users[user].DirEntries[p], entry)
}

func (this * EntrySet) Remove(id string) {
  entry := this.IdEntries[id]

  if entry == nil {
    return
  }

  delete(this.IdEntries, id)

  for i, e := range this.Entries {
    if e.Id == id {
      this.Entries = append(this.Entries[:i], this.Entries[i+1:]...)
      break
    }
  }

  user := entry.Owner
  name := entry.Name
  p := entry.Parent

  delete(this.Users[user].NameEntries, name)
  delete(this.Users[user].IdEntries, id)
  for i, e := range this.Users[user].DirEntries[p] {
    if e.Id == id {
      this.Users[user].DirEntries[p] = append(this.Users[user].DirEntries[p][:i], this.Users[user].DirEntries[p][i+1:]...)
      break
    }
  }
}

func NewEntrySet(entries []*MetaEntry, remote bool) * EntrySet {
  set := &EntrySet{ Entries: entries, IsRemote: remote }


  id_entries := make(map[string]*MetaEntry)
  user_entries := make(map[string]*UserEntries)

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
    if id != p {
      user.DirEntries[p] = append(user.DirEntries[p], entry)
    }
  }
  set.IdEntries = id_entries
  set.Users = user_entries

  return set
}

func (this * EntrySet) FromId(id string) * MetaEntry {
  return this.IdEntries[id]
}

func (this * EntrySet) FromName(user string, name string) * MetaEntry {
  return this.Users[user].NameEntries[name]
}

func (this * EntrySet) Rename(id, updated string) {
  if this.IdEntries[id] != nil {
    rename := this.IdEntries[id]

    user := rename.Owner

    this.Remove(id)

    if file := this.FromName(user, updated); file != nil {
      this.Remove(file.Id)
    }

    rename.Name = updated

    this.Add(rename)
  }
}

type FileStore struct {
  Path string
  Size int64
  Store * dumbstore.FileSystem
  Entries *EntrySet
  Remote map[string]*EntrySet
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
  Owner string `json:"owner"`
  Version int64 `json:"version"`
  Remote bool
  RemoteHost string
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

func parse_metadata(buf []byte, remote bool) (*EntrySet) {
  var entries []*MetaEntry
  err := json.Unmarshal(buf, &entries)

  if err != nil {
    panic(err)
  }

  return NewEntrySet(entries, remote)
}

func parse_remote_metadata(id string, buf []byte) (*EntrySet) {
  entries := parse_metadata(buf, true)
  entries.RemoteId = id

  for _, e := range entries.Entries {
    e.Remote = true
    e.RemoteHost = id
  }

  return entries
}

func load_metadata(name string) (*EntrySet) {
  buf := read_file("./" + name +  "_meta.json")
  if buf == nil {
    buf = []byte("[]")
  }

  return parse_metadata(buf, false)
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

  entries := load_metadata(name)

  remote_entries := make(map[string]*EntrySet)

  file_store := &FileStore{
    name,
    size,
    store,
    entries,
    remote_entries,
  }

  for name, user := range file_store.Entries.Users {
    if user.NameEntries["/"] == nil {
      file_store.CreateFile(name, "/", 16822, true)
    }
  }

  return file_store
}

func (this * FileStore) Close() {
  this.Save()
}

func (this * FileStore) Serialize() []byte {
  entries := make([]*MetaEntry, 0)

  entries = append(entries, this.Entries.Entries...)

  for _, remote := range this.Remote {
    for _, entry := range remote.Entries {
      unique := true

      for _, existing := range entries {
        if existing.Id == entry.Id {
          unique = false


          if entry.Version > existing.Version {
            existing.Version = entry.Version
          }

        }
      }

      if unique {
        entries = append(entries, entry)
      }
    }
  }

  buf, err := json.Marshal(entries)
  if err != nil {
    panic(err)
  }

  return buf
}

func (this *FileStore) CreateFile(user string, name string, mode uint16, dir bool) string {

  if this.Entries.Users[user].NameEntries[name] != nil {
    return this.Entries.Users[user].NameEntries[name].Id
  }

  f_type := "File"

  if dir {
    f_type = "Dir"
  }

  fmt.Printf("Creating %s (%s)\n", f_type, name)

  // fmt.Println(this.NameEntries)

  if name == "ROOT" && this.Entries.IdEntries["ROOT"] != nil {
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

    if this.Entries.Users[user].NameEntries[parent] == nil && name != "/" {
      this.CreateFile(user, parent, 16877, true)
    }

    if parent == "/" {
      parent = "ROOT"
    } else {
      parent = this.Entries.Users[user].NameEntries[parent].Id
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
    false,
    "",
  }

  this.Entries.Add(entry)
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
  file := this.Entries.IdEntries[id]

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
  file.Version = file.Version + 1
  this.Save()
  // fmt.Printf("Wrote %d Bytes\n", written)
}

func (this * FileStore) ParseRemote(remote_id string, buf []byte) {
  remote_entries := parse_remote_metadata(remote_id, buf)

  this.Remote[remote_id] = remote_entries
}

func (this * FileStore) Read(id string, position int64, length int64) []byte {
  file := this.Entries.IdEntries[id]

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
  if this.Entries.IdEntries[id] == nil {
    return
  }

  file := this.Entries.IdEntries[id]

  for _, chunk := range file.Chunks {
    this.Store.Free(chunk)
  }

  this.Entries.Remove(id)

  this.Save()
}

func (this * FileStore) Truncate(user string, name string, size int64) {
  fmt.Println(this.Entries.Users, this.Entries.Users[user], user)

  if this.Entries.Users[user].NameEntries[name] == nil {
    return
  }

  file := this.Entries.Users[user].NameEntries[name]

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

func (this * FileStore) Readdir(user string, name string) (*Error, []string) {
  fmt.Printf("Readdir: User: %s, Name: %s\n", user, name)
  file := this.LatestName(user, name)

  // fmt.Println(name, file.Id, file.Parent, file.Dir, file)

  if file == nil {
    fmt.Printf("%s %s %+v\n", user, name, this.Entries.Users[user])
    fmt.Println(file)
    return ENOENT, nil
  } else if !file.Dir {
    fmt.Printf("%s, %s, %+v\n", name, user, file)
    return ENOTDIR, nil
  }

  dir_id := file.Id

  local_files := this.Entries.Users[user].DirEntries[dir_id]
  remote_files := make([]*MetaEntry, 0)

  for _, entries := range this.Remote {
    if entries.Users[user].DirEntries[dir_id] != nil {
      remote_files = append(remote_files, entries.Users[user].DirEntries[dir_id]...)
    }
  }

  de_duped := make([]*MetaEntry, len(local_files) + len(remote_files)) // Faster to alloc big and relase later

  copy(de_duped, local_files)

  base_index := len(local_files)

  for _, item := range remote_files {
    unique := true
    for _, existing := range de_duped[:base_index] {
      if item.Id == existing.Id {
        unique = false
        break
      }
    }

    if unique {
      de_duped[base_index] = item
      base_index++
    }
  }

  names := make([]string, base_index)

  for i, item := range de_duped[:base_index] {
    fmt.Println(item)
    index := strings.LastIndex(item.Name, "/")
    names[i] = item.Name[index + 1:]
  }

  return nil, names
}

func (this * FileStore) Save() {
  save_metadata(this.Path, this.Entries.Entries)
}

func (this * FileStore) CreateUser(user string) {
  if this.Entries.Users[user] == nil {
    fmt.Println("Creating User", user)
    this.Entries.Users[user] = &UserEntries{
      make(map[string]*MetaEntry),
      make(map[string]*MetaEntry),
      make(map[string][]*MetaEntry),
    }
  }

  for _, remote := range this.Remote {
    if remote.Users[user] == nil {
      remote.Users[user] = &UserEntries{
        make(map[string]*MetaEntry),
        make(map[string]*MetaEntry),
        make(map[string][]*MetaEntry),
      }
    }
  }

  this.CreateFile(user, "/", 16822, true)
  this.CreateFile(user, "Keyring", 33206, false)
}

func (this * FileStore) LatestId(id string) * MetaEntry {
  local_version := int64(-1)
  if this.Entries.IdEntries[id] != nil {
    local_version = this.Entries.IdEntries[id].Version
  }

  remote_version := int64(-1)
  best_remote := ""

  for remote_id, entries := range this.Remote {
    if entries.IdEntries[id] != nil {
      entry := entries.IdEntries[id]
      if entry.Version > remote_version {
        remote_version = entries.IdEntries[id].Version
        best_remote = remote_id
      }
    }
  }

  if local_version == - 1 && remote_version == -1 {
    return nil
  }

  if remote_version > local_version {
    return this.Remote[best_remote].IdEntries[id]
  } else {
    return this.Entries.IdEntries[id]
  }
}

func (this * FileStore) LatestName(user, name string) * MetaEntry {
  local_version := int64(-1)
  if this.Entries.Users[user].NameEntries[name] != nil {
    local_version = this.Entries.Users[user].NameEntries[name].Version
  }

  remote_version := int64(-1)
  best_remote := ""

  for remote_id, entries := range this.Remote {
    if entries.Users[user] != nil && entries.Users[user].NameEntries[name] != nil {
      entry := entries.Users[user].NameEntries[name]
      if entry.Version > remote_version {
        remote_version = entries.Users[user].NameEntries[name].Version
        best_remote = remote_id
      }
    }
  }

  if local_version == - 1 && remote_version == -1 {
    return nil
  }

  if remote_version > local_version {
    return this.Remote[best_remote].Users[user].NameEntries[name]
  } else {
    return this.Entries.Users[user].NameEntries[name]
  }
}

func (this * FileStore) Rename(id, name string) {
  if this.Entries.IdEntries[id] != nil {
    this.Entries.Rename(id, name)
  }

  for _, remote := range this.Remote {
    if remote.IdEntries[id] != nil {
      remote.Rename(id, name)
    }
  }
}

func (this * FileStore) UnlinkId(id string) {
  if this.Entries.IdEntries[id] != nil {
    this.Entries.Remove(id)
  }

  for _, remote := range this.Remote {
    if remote.IdEntries[id] != nil {
      remote.Remove(id)
    }
  }
}

func main() {
  // fmt.Println("Starting File Store")
  file_store := NewFileStore("data", SIZE)

  var id string

  if file_store.Entries.Users["*"].NameEntries["Dummy"] != nil {
    id = file_store.Entries.Users["*"].NameEntries["Dummy"].Id
  } else {
      id = file_store.CreateFile("Dummy", "*", 33206, true)
  }

  file := file_store.Entries.IdEntries[id]

  for i := 0; i < 1000; i++ {
    file_store.Write(id, file.Size, []byte("Hello World\nTesting\n"))
  }

  file_store.Close()
}
