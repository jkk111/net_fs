package cache

import (
  "sync"
)

type CacheEntry struct {
  data []byte
}

type CachePage struct {
  Blocks map[int64]*int64
}

type Cache struct {
  mutex * sync.Mutex
  Pages map[string]*CachePage
  Blocks []*CacheEntry
  Size int64
}

func NewCache(size int64) * Cache {
  pages := make(map[string]*CachePage)
  blocks := make([]*CacheEntry, size)

  return &Cache{
    mutex: sync.Mutex{},
    Pages: pages,
    Blocks: blocks,
    Size: size,
  }
}

func (this * Cache) add_page(page string) {
  if this.Pages[page] == nil {
    page_blocks := make(map[int64]*int64)
    this.Pages[page] = &CachePage{ page_blocks }
  }
}

func (this * Cache) shift(before int64) {
  for _, page := range this.Pages {
    for block, val := range page.Blocks {
      if *val < before {
        value := (*val + 1)
        page.Blocks[block] = &value
        if *page.Blocks[block] > this.Size {
          delete(page.Blocks, block)
        }
      }
    }
  }
}

func (this * Cache) Add(page string, block int64, data []byte) {
  this.mutex.Lock()
  defer this.mutex.Unlock()
  this.add_page(page)

  if this.Pages[page].Blocks[block] != nil {
    index := *this.Pages[page].Blocks[block]
    this.Blocks[index] = &CacheEntry { data }
    this.Get(page, block)
    return
  }

  this.shift(this.Size + 1);

  entry := &CacheEntry{ data }

  this.Blocks = append([]*CacheEntry{ entry }, this.Blocks...)


  v := this.Pages[page].Blocks[block]

  if v == nil {
    value := int64(0)
    this.Pages[page].Blocks[block] = &value
  }
}

func (this * Cache) Get(page string, block int64) []byte {
  this.mutex.Lock()
  defer this.mutex.Unlock()
  this.add_page(page)

  file := this.Pages[page]

  index_ptr := file.Blocks[block]

  if index_ptr == nil {
    return nil
  } else {
    index := *index_ptr
    pre := this.Blocks[:index]
    post := this.Blocks[index + 1:]

    block_data := this.Blocks[index]


    this.shift(index)
    *file.Blocks[block] = 0
    this.Blocks = append(append([]*CacheEntry { block_data }, pre...), post...)
    return block_data.data
  }

  return nil
}
