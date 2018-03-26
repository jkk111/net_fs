package dumbstore

import (
  "fmt"
)

const MAX_STORE_SIZE int64 = 1024 * 1024

func main() {
  fmt.Println("Starting")
  fs := New("./files", MAX_STORE_SIZE)
  fs.Resize(MAX_STORE_SIZE + 1024)

  fs.Write([]byte("Hello World"))

  buf := make([]byte, 65536)

  for i := 0; i < 65536; i++ {
    buf[i] = 0x8a
  }

  fs.Write(buf)

  fmt.Println(string(fs.ReadChunk(0)[:11]))

  fs.Close()
}