// Inter-Node Communication

package inc

import (
  "fmt"
  "os"
  "net/http"
  "io/ioutil"
  "strings"
  "time"
  "sync"
  "encoding/json"
  "github.com/gorilla/websocket"
  "github.com/satori/go.uuid"
)

const (
  DEFAULT_PORT = ":8090"
  MESSAGE_TYPE = websocket.TextMessage
  CONFIG_PATH = "./config.json"
  MAX_CONN_RETRIES int = 10
  RETRY_WAIT = 10 * time.Second
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
  sender * INCNode
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
  *sync.Mutex
  Id []byte
  router * INCRouter
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

// Type Signatures are hard :(
type INCRouter struct {
  Id []byte
  nodes map[string]*INCNode
  nodes_url map[string]*INCNode
  records map[string]*MessageRecord
  awaiting map[string]chan * INCMessage
  handlers map[string]chan * INCMessage
  connection_listeners []func(*INCNode)
  Bootstrap []string
  mchan chan * INCMessage
}

func (this * INCRouter) Await(mid string, ch chan * INCMessage) {
  this.awaiting[mid] = ch
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
  return []byte(uuid.Must(uuid.NewV4()).String())
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
  nodes := make(map[string]*INCNode)
  nodes_url := make(map[string]*INCNode)
  records := make(map[string] * MessageRecord)
  awaiting := make(map[string]chan * INCMessage)
  handlers := make(map[string]chan * INCMessage)

  connection_listeners := make([]func(*INCNode), 0)

  router := &INCRouter{
    Id: config.Id,
    Bootstrap: config.Bootstrap,
    mchan: message_chan,
    nodes: nodes,
    nodes_url: nodes_url,
    records: records,
    awaiting: awaiting,
    handlers: handlers,
    connection_listeners: connection_listeners,
  }

  return router
}

func NewINCRouter(port string) * INCRouter {
  config := load_config()
  router := create_router(config)
  return router
}

func (this * INCRouter) BootstrapNodes(bootstrap []string) {
  for _, node := range bootstrap {
    exists := false
    for _, bootstrapped := range this.Bootstrap {
      if bootstrapped == node {
        exists = true
      }
    }

    if !exists {
      success, retry := this.connect(node)

      if success {
        this.Bootstrap = append(this.Bootstrap, node)
      } else if retry {
        go this.connect_after(node, 1)
      }
    }
  }
}

func (this * INCRouter) connect_after(node string, retry_count int) {
  if retry_count > MAX_CONN_RETRIES {
    return
  }

  if retry_count < 1 {
    retry_count = 1
  }

  time.Sleep(time.Duration(retry_count) * RETRY_WAIT)

  success, retry := this.connect(node)

  if success {
    this.Bootstrap = append(this.Bootstrap, node)
  } else if retry {
    this.connect_after(node, retry_count + 1)
  }
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

func (this * INCRouter) On(evt string, ch chan * INCMessage) {
  this.handlers[evt] = ch
}

func (this * INCRouter) OnConnect(f func(*INCNode)) {
  this.connection_listeners = append(this.connection_listeners, f)
}

func (this * INCRouter) Receive() (*INCMessage) {
  return <- this.mchan
}

func (this * INCNode) Close() {

}

func (this * INCNode) Send(message * INCMessage) {
  this.Lock()
  this.conn.WriteMessage(MESSAGE_TYPE, message.Serialize())
  this.Unlock()
}

func (this * INCNode) handleMessages() {
  node_list, err := json.Marshal(this.router.Bootstrap)

  if err != nil {
    fmt.Println("Error: Failed to serialize node list")
  }

  msg := NewINCMessage("NODE_LIST", true, node_list)
  this.router.Emit(msg)

  // TODO()
  fmt.Println("here", len(this.router.connection_listeners))
  for _, ln := range this.router.connection_listeners {
    ln(this)
  }

  for {
    t, m, err := this.conn.ReadMessage()

    if t != MESSAGE_TYPE || err != nil {
      this.Close()
      return
    }

    msg := ParseMessage(m)
    msg.sender = this

    fmt.Println("Parsed Incoming Message", msg.Type)

    this.mchan <- msg
  }
}

func (this * INCRouter) handleMessages() {
  for {
    time.Sleep(time.Second)
    fmt.Println("awaiting!")
    msg := this.Receive()

    fmt.Println("Received Message %s (%s)", string(msg.Mid), msg.Type)

    // fmt.Println("Here", msg)
    _ = msg.sender.Id

    mid := string(msg.Mid)
    if this.records[mid] != nil {
      fmt.Println("Already Saw This Message", string(msg.Mid))
      continue
    } else {
      this.records[mid] = &MessageRecord{ time.Now() }
    }

    rid := string(msg.Rid)

    if this.awaiting[rid] != nil {
      ln := this.awaiting[rid]
      delete(this.awaiting, rid)
      ln <- msg
      continue
    }

    m_type := msg.Type

    if this.handlers[m_type] != nil {
      fmt.Println("Handler for type", m_type)
      this.handlers[m_type] <- msg
    } else {
      fmt.Println("No Handler For Type", m_type)
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
  fmt.Printf("Remote Address: %s\n", req.RemoteAddr)

  request_parts := strings.Split(req.RemoteAddr, ":")
  remote_url := request_parts[0] + DEFAULT_PORT

  conn, err := upgrader.Upgrade(w, req, nil)

  if err != nil {
    return
  }


  conn.SetReadDeadline(time.Now().Add(5 * time.Second))
  t, m, err := conn.ReadMessage()

  if t != MESSAGE_TYPE || err != nil {
    return
  }

  // fmt.Println(m)

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

  node := &INCNode{ &sync.Mutex{}, id, this, conn, this.mchan }
  this.nodes[string(id)] = node
  this.nodes_url[remote_url] = node

  this.Bootstrap = append(this.Bootstrap, remote_url)

  go node.handleMessages()
}

func (this * INCRouter) connect(url string) (success bool, retry bool) {
  fmt.Println("Connecting to", url)

  raw_url := url

  url = fmt.Sprintf("ws://%s/ws", url)


  conn, _, err := websocket.DefaultDialer.Dial(url, nil)

  if err != nil {
    fmt.Println(err)
    return false, true
  }

  fmt.Println("Connected\nHandshaking")

  chello := INCHello{ this.Id }
  cmsg, _ := json.Marshal(chello)
  message := NewINCMessage("HELLO", false, cmsg)

  conn.WriteMessage(MESSAGE_TYPE, message.Serialize())
  conn.SetReadDeadline(time.Now().Add(5 * time.Second))

  t, m, err := conn.ReadMessage()

  if t != MESSAGE_TYPE || err != nil {
    return false, false
  }

  parsed := ParseMessage(m)

  if parsed.Type != "HELLO" {
    return false, false
  }

  var shello INCHello
  err = json.Unmarshal(parsed.Message, &shello)

  if err != nil {
    return false, false
  }

  id := shello.Id

  if string(id) == string(this.Id) {
    conn.Close()
    return false, false
  }

  fmt.Println("Successful Handshake", id)

  conn.SetReadDeadline(time.Time{})
  node := &INCNode{ &sync.Mutex{}, id, this, conn, this.mchan }
  this.nodes[string(id)] = node
  this.nodes_url[raw_url] = node

  go node.handleMessages()
  return true, false
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
      this.Send(string(node.Id), message)
    }
  })()
}

func (this * INCRouter) Emit(message * INCMessage) {
  message.Id = this.Id
  this.emit(message)
}

func (this * INCRouter) Send(node string, message * INCMessage) {
  message.Id = this.Id

  this.records[string(message.Mid)] = &MessageRecord{ time.Now() }

  fmt.Println(this.nodes, node)

  this.nodes[node].Send(message)
}
