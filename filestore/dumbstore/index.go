package dumbstore

import (
  "sync"
  "os"
  "math"
  "fmt"
  "errors"
)

var /* Errors */ (
  E_FULL = errors.New("Block Storage Is Full")
  E_NOT_SUP = errors.New("Function Not Currently Supported")
)

const (
  WRITE_SIZE int64 = 1024 * 256 // This Can Be Changed, But Needs To Be Standardized
  SEEK_MODE int = 0 // Seek From The Start Of The File
)

func init() {
  fmt.Println("Initializing Alpha Blob Storage Driver")
}

type FileSystem struct {
  *sync.Mutex
  handle * os.File
  offset int64
  size int64
  availability []bool
}

type WriteRecord struct {
  Chunk int64
  Length int64
}

func ComputeChunks(size int64) int64 {
  tmp := float64(size) / float64(WRITE_SIZE)
  tmp = math.Ceil(tmp)
  value := int64(tmp)
  if value % 8 > 0 {
    value += value % 8
  }
  return int64(value)
}

func ComputeSize(size int64) int64 {
  chunks := ComputeChunks(size)
  return (chunks / 8) + (chunks * WRITE_SIZE)
}

func openFileSystem(path string) * os.File {
  f, e := os.OpenFile(path, os.O_RDWR, 0755)

  if e != nil {
    panic(e)
  }

  return f
}

func new_fs(handle * os.File, size int64) * FileSystem {
  bytes := ComputeChunks(size) / 8
  size = ComputeSize(size)

  // fmt.Printf("Offsetting %d bytes\n", bytes)

  availability := make([]bool, bytes * 8)

  a_count := 0
  c_count := 0

  fs := &FileSystem{ &sync.Mutex{}, handle, bytes, size, availability }

  for i := int64(0); i < bytes; i++ {
    buf := make([]byte, 1)
    fs.Seek(i)
    fs.handle.Read(buf)

    for j := uint64(0); j < 8; j++  {
      mask := byte(0x80 >> j)
      val := mask & buf[0]

      if val == 0 {
        index := i * 8 + int64(j)
        availability[index] = true
        a_count ++
      }

      c_count++
    }
  }

  // fmt.Println(a_count, c_count, availability[:8], len(availability))

  return fs
}

func (this * FileSystem) LowerBoundChunk() int64 {
  this.Seek(0)
  buf := make([]byte, 1)

  for i := int64(0); i < this.offset; i++ {
    this.handle.Read(buf)

    if buf[0] > 0 {

      for off := byte(0); off < 8; off++ {
        mask := byte(0x80 >> off)

        result := buf[0] & mask
        if result > 0 {
          return (i * 8) + int64(off)
        }
      }

    }
  }
  return (this.offset * 8) - 1
}

func (this * FileSystem) UpperBoundChunk() int64 {
  this.Seek(0)
  buf := make([]byte, 1)

  upperBound := (this.offset * 8) -1

  for i := int64(0); i < this.offset; i++ {
    this.handle.Read(buf)

    if buf[0] > 0 {
      for off := byte(0); off < 8; off++ {
        mask := byte(0x80 >> off)

        result := buf[0] & mask
        if result > 0 {
          upperBound = (i * 8) + int64(off)
        }
      }
    }
  }
  return upperBound
}

func (this * FileSystem) Resize(size int64) {
  size = ComputeSize(size)
  if size < this.size {
    panic(E_NOT_SUP)
  } else if size == this.size {
    return
  }

  lower := this.LowerBoundChunk()
  upper := this.UpperBoundChunk()

  // byte_count := (upper - lower + 1) * WRITE_SIZE

  // fmt.Printf("Need To Move Chunks %d - %d (%d Bytes)\n", lower, upper, byte_count)
  // fmt.Printf("Resizing From %d Bytes to %d Bytes\n", this.size, size)

  tmp := create_fs(this.handle.Name() + ".tmp", size)

  for i := lower; i <= upper; i++ {
    tmp.Reserve(i)
    tmp.Overwrite(i, this.ReadChunk(i))
  }

  tmp.Close()
  this.handle.Close()

  os.Rename(tmp.handle.Name(), this.handle.Name())
  this.handle = openFileSystem(this.handle.Name())
}

func (this * FileSystem) ResizeUnsafe(size int64) {
  this.handle.Truncate(size)
}

func (this * FileSystem) Close() {
  this.handle.Close()
}

func New(path string, max_size int64) * FileSystem {
  if max_size % WRITE_SIZE > 0 {
    max_size -= max_size % WRITE_SIZE
  }
  return create_fs(path, max_size)
}

// Creates a filesystem of size max_size, if already exists ignores the size
func create_fs(path string, max_size int64) * FileSystem {
  var handle * os.File
  _, e := os.Stat(path)
  if e != nil {
    if os.IsNotExist(e) {
      f, e := os.Create(path)
      if e != nil {
        panic(e)
      }

      max_size = ComputeSize(max_size)

      handle = f
      handle.Truncate(max_size)
    } else {
      panic(e)
    }
  } else {
    // fmt.Printf("%+v\n", stat)
    f := openFileSystem(path)
    handle = f
  }

  return new_fs(handle, max_size)
}

func get_bit(b byte) int64 {
  mask := byte(0x80) // Golang needs to support binary literals!
  for i := byte(0); i < 8; i++ {
    set := b & (mask >> i)
    if set == 0 {
      return int64(i)
    }
  }
  panic("Oh Bother")
  return -1
}

func (this * FileSystem) GetNextAvailable() int64 {
  this.Seek(0)

  for i, v := range this.availability {
    if v {
      return int64(i)
    }
  }

  return -1

  i := int64(0)
  for i < this.offset {
    buf := make([]byte, 8)

    n, _ := this.handle.Read(buf)
    buf = buf[:n]

    for ci, c := range(buf) {
      // fmt.Println("Current", c, ci)
      if c < 0xff {
        bit := get_bit(c)

        if bit < 0 {
          return -1
        }

        return bit + (i * 8) + int64(ci * 8)
      }
    }

    i += int64(len(buf))
  }

  // fmt.Println("Failed", i, this.offset)

  return -1 // Failure condition
}

func (this * FileSystem) Seek(offset int64) {
  this.handle.Seek(offset, SEEK_MODE)
}


func (this * FileSystem) Reserve(chunk int64) {
  // fmt.Println("Reserving Chunk", chunk)
  c_byte := chunk / 8
  c_offset := byte(chunk % 8)

  // fmt.Printf("Reserving Byte: %d Bit: %d\n", c_byte, c_offset)

  mask := byte(0x80 >> c_offset)

  // fmt.Println("Mask", mask)

  this.Seek(c_byte)
  buf := make([]byte, 1)
  this.handle.Read(buf)
  curr := buf[0]
  curr = curr | mask
  buf[0] = curr

  // fmt.Printf("Buffer %d, Current %d, Mask %d\n", buf[0], curr, mask)

  this.handle.Seek(-1, 1) // We seek backwards here rather than going from the start of the file again!
  this.handle.Write(buf)
  this.availability[chunk] = false
}

func (this * FileSystem) Free(chunk int64) {
  c_byte := chunk / 8
  c_offset := byte(chunk % 8)
  mask := byte(0x80 >> c_offset)
  mask = byte(0xff) ^ mask
  this.Seek(c_byte)
  buf := make([]byte, 1)
  this.handle.Read(buf)
  curr := buf[0]
  curr = curr & mask
  buf[0] = curr
  this.handle.Seek(-1, 1) // We seek backwards here rather than going from the start of the file again!
  this.handle.Write(buf)

  // Don't forget to zero the chunk just in case
  offset := chunk * WRITE_SIZE + this.offset
  tmp := make([]byte, WRITE_SIZE)
  this.Overwrite(offset, tmp)
  this.availability[chunk] = true
}

func (this * FileSystem) Write(data []byte) []int64 {
  this.Lock()
  l := int64(len(data))
  if l == 0 {
    return nil
  }

  raw_chunks := float64(len(data)) / float64(WRITE_SIZE)
  chunks := int64(math.Ceil(raw_chunks))

  chunk_ids := make([]int64, chunks)

  for i := int64(0); i < chunks; i++ {
    chunk := this.GetNextAvailable()

    start := i * WRITE_SIZE
    end := start + WRITE_SIZE

    // fmt.Printf("Writing %d bytes to %d\n", end - start, chunk)

    if end > l {
      end = l
    }

    if chunk != -1 {
      this.Reserve(chunk)
      this.Overwrite(chunk, data[start:end])
      chunk_ids[i] = chunk
    } else {
      panic(E_FULL)
    }
  }
  this.Unlock()
  return chunk_ids
}

func (this * FileSystem) Overwrite(chunk int64, data []byte) {
  if int64(len(data)) > WRITE_SIZE {
    panic("Shit")
  }

  this.Seek((chunk * WRITE_SIZE) + this.offset)
  this.handle.Write(data)
}

func (this * FileSystem) OverwriteOffset(chunk, offset int64, data []byte) {
  if offset == 0 {
    this.Overwrite(chunk, data)
  }
  this.Seek((chunk * WRITE_SIZE) + this.offset + offset)
  this.handle.Write(data)
}

func (this * FileSystem) Read(offset, length int64) []byte {
  this.Lock()
  this.Seek(offset + this.offset)
  buf := make([]byte, length)
  this.handle.Read(buf)
  this.Unlock()
  return buf
}

func (this * FileSystem) ReadChunk(chunk int64) []byte {
  offset := chunk * WRITE_SIZE
  return this.Read(offset, WRITE_SIZE)
}

func compute_real_offset(offset int64) int64 {
  tmp := float64(offset)
  tmp_ws := float64(WRITE_SIZE)

  computed := int64(math.Ceil(tmp / tmp_ws)) * WRITE_SIZE
  // fmt.Println("Real Offset", computed)
  return computed
}

func (this * FileSystem) write(offset int64, data []byte) WriteRecord {
  offset = compute_real_offset(offset)
  this.handle.Seek(offset, 0)
  // fmt.Println(this.handle.Write(data))

  wr := WriteRecord{ Chunk: offset / WRITE_SIZE }

  return wr
}

func (this * FileSystem) WriteRaw(chunk, c_offset int64, data []byte) {
  if c_offset + int64(len(data)) > WRITE_SIZE {
    panic("Fuck Fam")
  } else {
    offset := chunk * WRITE_SIZE
    offset += c_offset

    this.handle.Seek(offset, 0)
    this.handle.Write(data)
  }
}

// This should be rethought, merged writes will be needed!
// Although since this is a "dumb" store we can leave that to server to handle
// func (this * FileSystem) Write(offset int64, data []byte) []WriteRecord {
//   // Three Situations here
//   l := int64(len(data))
//   if l < WRITE_SIZE { // 1 Buf is too small
//     buf := make([]byte, WRITE_SIZE)
//     copy(buf, data)
//     o := this.Write(offset, buf)
//     o[0].Length = l
//     return []WriteRecord { o[0] }
//   } else if l > WRITE_SIZE { // 2 Buf is too large
//     chunks := int64(l / WRITE_SIZE)
//     var i int64
//     offsets := make([]WriteRecord, chunks + 1)
//     for i = 0; i <= chunks; i++ {
//       start := WRITE_SIZE * i
//       end := start + WRITE_SIZE

//       if end > l {
//         end = l
//       }

//       buf := data[start:end]
//       o := this.Write(offset + start, buf)
//       o[0].Length = end - start
//       offsets[i] = o[0]
//     }
//     return offsets

//   } else { // Well sized, no prep needed, just write
//     o := this.write(offset, data)
//     o.Length = l
//     return []WriteRecord { o }
//   }
// }
