package ddp

import "log"

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
	return &KeyCache{name, map[string]interface{}{}}
}

// KeyCache caches items keyed on unique ID.
type KeyCache struct {
	// The name of the collection
	Name string
	// items contains collection items by ID
	items map[string]interface{}
}

func (c *KeyCache) added(msg map[string]interface{}) {
	id := idForMessage(msg)
	c.items[id] = msg["fields"]
	log.Printf("db.%s insert %s\n", c.Name, id)
}

func (c *KeyCache) changed(msg map[string]interface{}) {
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
}

func (c *KeyCache) removed(msg map[string]interface{}) {
	id := idForMessage(msg)
	delete(c.items, id)
	log.Printf("db.%s remove %s\n", c.Name, id)
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
