package ddp

import (
	"fmt"
	"log"
)

// ----------------------------------------------------------------------
// EJSON document interface
// ----------------------------------------------------------------------

// Doc provides hides the complexity of ejson documents.
type Doc struct {
	root interface{}
}

// NewDoc creates a new document from a generic json parsed document.
func NewDoc(in interface{}) *Doc {
	doc := &Doc{in}
	return doc
}

// GetMapForPath locates a map[string]interface{} - json object - at a path
// or returns an error.
func (d *Doc) GetMapForPath(path []string) (map[string]interface{}, error) {
	item, err := d.ItemForPath(path)
	if err != nil {
		return nil, err
	}
	switch m := item.(type) {
	case map[string]interface{}:
		return m, nil
	default:
		return nil, fmt.Errorf("Non-map found at path %+v", item)
	}
}

// MapForPath sets a map[string]interface{} - json object - at a path or returns
// an error.
func (d *Doc) MapForPath(path []string, value map[string]interface{}) error {
	dir, key, err := d.DirForPath(path)
	if err != nil {
		return err
	}
	dir[key] = value
	return nil
}

// GetStringForPath returns a string value for the path or an error if the
// string was found.
func (d *Doc) GetStringForPath(path []string) (string, error) {
	dir, key, err := d.DirForPath(path)
	if err != nil {
		return "", err
	}
	value := dir[key]
	switch v := value.(type) {
	case string:
		return v, nil
	default:
		return "", fmt.Errorf("Item at path was not a string, found %+v instead", value)
	}
}

// StringForPath sets a string value on the path.
// Returns a non-nil error if the value could not be set.
func (d *Doc) StringForPath(value, path []string) error {
	dir, key, err := d.DirForPath(path)
	if err != nil {
		return err
	}
	dir[key] = value
	return nil
}

// ItemForPath locates the "raw" item at the provided path, returning
// the item found or an error.
func (d *Doc) ItemForPath(path []string) (item interface{}, err error) {
	item = d.root
	for _, step := range path {
		// This is an intermediate step - we must be in a map
		switch m := item.(type) {
		case map[string]interface{}:
			item = m[step]
		default:
			return nil, fmt.Errorf("Path contains non-object %s", step)
		}
	}
	return item, nil
}

// DirForPath locates a map[string]interface{} - json object - at the
// directory contained in the path and returns the map and key/file name
// specified or an error.
func (d *Doc) DirForPath(path []string) (dir map[string]interface{}, key string, err error) {
	var parent []string
	parent, key, err = d.Split(path)
	if err != nil {
		return nil, "", err
	}
	dir, err = d.GetMapForPath(parent)
	if err != nil {
		return nil, "", err
	}
	return
}

// Split splits a path returning the parts of the path directory and the
// last item (key/file). An empty path results in an error.
func (d *Doc) Split(path []string) (dir []string, file string, err error) {
	if len(path) == 0 {
		return nil, "", fmt.Errorf("Empty path")
	}
	last := len(path) - 1
	dir = path[:last]
	file = path[last]
	err = nil
	return
}

// ----------------------------------------------------------------------
// Collection
// ----------------------------------------------------------------------

// Collection managed cached collection data sent from the server in a
// livedata subscription.
//
// It would be great to build an entire mongo compatible local store (minimongo)
type Collection interface {

	// FindOne queries objects and returns the first match.
	FindOne(id string) interface{}
	// FindAll returns a map of all items in the cache - this is a hack
	// until we have time to build out a real minimongo interface.
	FindAll() map[string]interface{}
	// AddUpdateListener adds a channel that receives update messages.
	AddUpdateListener(chan<- map[string]interface{})

	// livedata updates

	added(msg map[string]interface{})
	changed(msg map[string]interface{})
	removed(msg map[string]interface{})
	addedBefore(msg map[string]interface{})
	movedBefore(msg map[string]interface{})
}

// NewMockCollection creates an empty collection that does nothing.
func NewMockCollection() Collection {
	return &MockCache{}
}

// NewCollection creates a new collection - always KeyCache.
func NewCollection(name string) Collection {
	return &KeyCache{name, map[string]interface{}{}, nil}
}

// KeyCache caches items keyed on unique ID.
type KeyCache struct {
	// The name of the collection
	Name string
	// items contains collection items by ID
	items map[string]interface{}
	// listeners contains all the listeners that should be notified of collection updates.
	listeners []chan<- map[string]interface{}
	// TODO(badslug): do we need to protect from multiple threads
}

func (c *KeyCache) added(msg map[string]interface{}) {
	log.Println("added", msg)
	id := idForMessage(msg)
	c.items[id] = msg["fields"]
	// TODO(badslug): change notification should include change type
	for _, listener := range c.listeners {
		log.Println("notifying listener", listener)
		listener <- msg
	}
	log.Println("added done")
}

func (c *KeyCache) changed(msg map[string]interface{}) {
	log.Println("changed", msg)
	id := idForMessage(msg)
	item, ok := c.items[id]
	if ok {
		switch itemFields := item.(type) {
		case map[string]interface{}:
			fields, ok := msg["fields"]
			if ok {
				switch msgFields := fields.(type) {
				case map[string]interface{}:
					for key, value := range msgFields {
						itemFields[key] = value
					}
					c.items[id] = itemFields
				default:
					// Don't know what to do...
				}
			}
		default:
			// Don't know what to do...
		}
	} else {
		c.items[id] = msg["fields"]
	}
	for _, listener := range c.listeners {
		log.Println("notifying listener", listener)
		listener <- msg
	}
	log.Println("changed done")
}

func (c *KeyCache) removed(msg map[string]interface{}) {
	id := idForMessage(msg)
	delete(c.items, id)
}

func (c *KeyCache) addedBefore(msg map[string]interface{}) {
	// for keyed cache, ordered commands are a noop
}

func (c *KeyCache) movedBefore(msg map[string]interface{}) {
	// for keyed cache, ordered commands are a noop
}

// FindOne returns the item with matching id.
func (c *KeyCache) FindOne(id string) interface{} {
	return c.items[id]
}

// FindAll returns a dump of all items in the collection
func (c *KeyCache) FindAll() map[string]interface{} {
	return c.items
}

// AddUpdateListener adds a listener for changes on a collection.
func (c *KeyCache) AddUpdateListener(ch chan<- map[string]interface{}) {
	c.listeners = append(c.listeners, ch)
}

// OrderedCache caches items based on list order.
// This is a placeholder, currently not implemented as the Meteor server
// does not transmit ordered collections over DDP yet.
type OrderedCache struct {
	// ranks contains ordered collection items for ordered collections
	items []interface{}
}

func (c *OrderedCache) added(msg map[string]interface{}) {
	// for ordered cache, key commands are a noop
}

func (c *OrderedCache) changed(msg map[string]interface{}) {

}

func (c *OrderedCache) removed(msg map[string]interface{}) {

}

func (c *OrderedCache) addedBefore(msg map[string]interface{}) {

}

func (c *OrderedCache) movedBefore(msg map[string]interface{}) {

}

// FindOne returns the item with matching id.
func (c *OrderedCache) FindOne(id string) interface{} {
	return nil
}

// FindAll returns a dump of all items in the collection
func (c *OrderedCache) FindAll() map[string]interface{} {
	return map[string]interface{}{}
}

// AddUpdateListener does nothing.
func (c *OrderedCache) AddUpdateListener(ch chan<- map[string]interface{}) {
}

// MockCache implements the Collection interface but does nothing with the data.
type MockCache struct {
}

func (c *MockCache) added(msg map[string]interface{}) {

}

func (c *MockCache) changed(msg map[string]interface{}) {

}

func (c *MockCache) removed(msg map[string]interface{}) {

}

func (c *MockCache) addedBefore(msg map[string]interface{}) {

}

func (c *MockCache) movedBefore(msg map[string]interface{}) {

}

// FindOne returns the item with matching id.
func (c *MockCache) FindOne(id string) interface{} {
	return nil
}

// FindAll returns a dump of all items in the collection
func (c *MockCache) FindAll() map[string]interface{} {
	return map[string]interface{}{}
}

// AddUpdateListener does nothing.
func (c *MockCache) AddUpdateListener(ch chan<- map[string]interface{}) {
}

func idForMessage(msg map[string]interface{}) string {
	id, ok := msg["id"]
	if ok {
		switch key := id.(type) {
		case string:
			return key
		}
	}
	return ""
}
