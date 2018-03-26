// Inter-Node Communication

package inc

import (
  "fmt"
  "os"
  "net/http"
  "io/ioutil"
  "crypto/rand"
  "time"
  "encoding/json"
  "github.com/gorilla/websocket"
)

const (
  MESSAGE_TYPE = websocket.TextMessage
  CONFIG_PATH = "./config.json"
)

var upgrader = websocket.Upgrader{
  ReadBufferSize:  1024,
  WriteBufferSize: 1024,
}

type INCMessage struct {
  Id []byte `json:"id"` // Sender ID
  Mid []byte `json:"mid"` // Message ID Random - Used to elimate duplicates
  Rid []byte `json:"rid"` // Response ID
  Dest []byte `json:"dest"` // (Optional): Defines a destination node
  Type string `json:"type"` // (Optional): Defines a destination node
  Echo bool `json:"echo"` // Should message be echoed
  Message []byte `json:"message"` // Message to proxy
  sender * INCMessage
}

type INCHello struct {
  Id []byte
}

func (this * INCMessage) Serialize() []byte {
  serialized, err := json.Marshal(this)

  if err != nil {
    panic(err)
  }

  return serialized
}

type INCNode struct {
  Id []byte
  conn * websocket.Conn
  mchan chan * INCMessage
}

type MessageRecord struct {
  received time.Time
}

type INCConfig struct {
  Id []byte `json:"id"`
  Bootstrap []string `json:"bootstrap"`
}

type INCRouter struct {
  Id []byte
  nodes map[string]*INCNode
  records map[string]*MessageRecord
  awaiting map[string]chan * INCMessage
  handlers map[string]chan * INCMessage
  Bootstrap []string
  mchan chan * INCMessage
}

func NewINCMessage(m_type string, echo bool, message []byte) *INCMessage {
  _ = websocket.DefaultDialer
  mid := random_id()
  var dest []byte
  dest = nil
  return &INCMessage{
    Mid: mid,
    Dest: dest,
    Type: m_type,
    Echo: echo,
    Message: message,
  }
}

func random_id() []byte {
  buf := make([]byte, 32)
  rand.Read(buf)
  return buf
}

func load_default_config() * INCConfig {
  return &INCConfig{
    Id: random_id(),
    Bootstrap: make([]string, 0),
  }
}

func load_config() * INCConfig {
  f, err := os.Open(CONFIG_PATH)

  if err != nil {
    return load_default_config()
  }

  buf, err := ioutil.ReadAll(f)

  if err != nil {
    return load_default_config()
  }

  var config INCConfig
  err = json.Unmarshal(buf, &config)

  if err != nil {
    return load_default_config()
  }

  return &config
}

func create_router(config * INCConfig) * INCRouter {
  message_chan := make(chan * INCMessage)
  router := &INCRouter{
    Id: config.Id,
    Bootstrap: config.Bootstrap,
    mchan: message_chan,
  }

  return router
}

func NewINCRouter(port string, bootstrap []string) * INCRouter {
  config := load_config()
  router := create_router(config)
  for _, node := range bootstrap {
    exists := false
    for _, bootstrapped := range router.Bootstrap {
      if bootstrapped == node {
        exists = true
      }
    }

    if !exists {
      router.connect(node)
      router.Bootstrap = append(router.Bootstrap, node)
    }
  }
  return router
}

// I could map out all nodes + paths or I could just check if I've already received this message
// More memory to store message ids and timestamps, less CPU
// Alt: Spikes in CPU + travelling salesman problem + Huge memory usage to store a map
func (this * INCRouter) clearRecords() {
  for {
    time.Sleep(5 * time.Minute)
    for key, record := range this.records {
      if time.Since(record.received) > 5 * time.Minute {
        delete(this.records, key)
      }
    }
  }
}

func (this * INCRouter) Receive() (*INCMessage) {
  return <- this.mchan
}

func (this * INCNode) Close() {

}


func (this * INCNode) Send(message * INCMessage) {
  this.conn.WriteMessage(MESSAGE_TYPE, message.Serialize())
}

func (this * INCNode) handleMessages() {
  for {
    t, m, err := this.conn.ReadMessage()

    if t != MESSAGE_TYPE || err != nil {
      this.Close()
      return
    }

    msg := ParseMessage(m)
    this.mchan <- msg
  }
}

func (this * INCRouter) handleMessages() {
  for {
    time.Sleep(time.Second)
    msg := this.Receive()

    _ = msg.sender.Id

    mid := string(msg.Mid)
    if this.records[mid] != nil {
      continue
    }

    rid := string(msg.Rid)

    if this.awaiting[rid] != nil {
      this.awaiting[rid] <- msg
      delete(this.awaiting, rid)
    }

    m_type := msg.Type

    if this.handlers[m_type] != nil {
      this.handlers[m_type] <- msg
    }

    dest := string(msg.Dest)
    if msg.Echo {
      if dest != "" && len(dest) > 0 && this.nodes[dest] != nil {
        this.nodes[dest].Send(msg)
      } else {
        this.emit(msg)
      }
    }
  }
}

func (this * INCRouter) HandleIncoming(w http.ResponseWriter, req * http.Request) {
  conn, err := upgrader.Upgrade(w, req, nil)

  if err != nil {
    return
  }


  conn.SetReadDeadline(time.Now().Add(5 * time.Second))
  t, m, err := conn.ReadMessage()

  if t != MESSAGE_TYPE || err != nil {
    return
  }

  fmt.Println(m)

  parsed := ParseMessage(m)

  if parsed.Type != "HELLO" {
    return
  }


  var chello INCHello
  err = json.Unmarshal(parsed.Message, &chello)

  if err != nil {
    return
  }

  id := chello.Id

  shello := INCHello { this.Id }
  smsg, _ := json.Marshal(shello)
  message := NewINCMessage("HELLO", false, smsg)

  conn.WriteMessage(MESSAGE_TYPE, message.Serialize())
  conn.SetReadDeadline(time.Time{})

  node := &INCNode{ id, conn, this.mchan }
  this.nodes[string(id)] = node
  go node.handleMessages()
}

func (this * INCRouter) connect(url string) {
  fmt.Println("Connecting to", url)

  url = fmt.Sprintf("ws://%s", url)


  conn, _, err := websocket.DefaultDialer.Dial(url, nil)

  if err != nil {
    fmt.Println(err)
    return
  }

  fmt.Println("Connected\nHandshaking")

  chello := INCHello{ this.Id }
  cmsg, _ := json.Marshal(chello)
  message := NewINCMessage("HELLO", false, cmsg)

  conn.WriteMessage(MESSAGE_TYPE, message.Serialize())
  conn.SetReadDeadline(time.Now().Add(5 * time.Second))

  t, m, err := conn.ReadMessage()

  if t != MESSAGE_TYPE || err != nil {
    return
  }

  parsed := ParseMessage(m)

  if parsed.Type != "HELLO" {
    return
  }

  var shello INCHello
  err = json.Unmarshal(parsed.Message, &shello)

  if err != nil {
    return
  }

  id := shello.Id

  fmt.Println("Successful Handshake", id)

  conn.SetReadDeadline(time.Time{})
  node := &INCNode{ id, conn, this.mchan }
  this.nodes[string(id)] = node

  go node.handleMessages()
}

func (this * INCRouter) HandleMessages() {
  go this.handleMessages()
  go this.clearRecords()
}

func ParseMessage(msg []byte) *INCMessage {
  var message INCMessage
  err := json.Unmarshal(msg, &message)

  if err != nil {
    panic(err)
  }

  return &message
}

func (this * INCRouter) emit(message * INCMessage) {
  returned := false
  _ = returned
  go (func() {
    for _, node := range this.nodes {
      node.Send(message)
    }
  })()
}

func (this * INCRouter) Emit(message * INCMessage) {
  message.Id = this.Id
  this.emit(message)
}