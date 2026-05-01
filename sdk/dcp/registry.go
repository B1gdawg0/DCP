package dcp

import (
	"hash/fnv"
	"sync"
)

// route key is service+operation+version combined
type routeKey struct {
	ServiceID uint32
	OpID      uint32
	Version   uint8
}

type HandlerFunc func(req *Request) (*Response, error)

type registry struct {
	mu       sync.RWMutex
	handlers map[routeKey]HandlerFunc
	names    map[routeKey][3]string // for debugging: service, op, version string
}

func newRegistry() *registry {
	return &registry{
		handlers: make(map[routeKey]HandlerFunc),
		names:    make(map[routeKey][3]string),
	}
}

func (r *registry) register(service, operation string, version uint8, fn HandlerFunc) {
	key := routeKey{
		ServiceID: hashName(service),
		OpID:      hashName(operation),
		Version:   version,
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[key] = fn
	r.names[key] = [3]string{service, operation, ""}
}

func (r *registry) resolve(serviceID, opID uint32, version uint8) (HandlerFunc, bool) {
	key := routeKey{serviceID, opID, version}
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.handlers[key]
	return fn, ok
}

// hashName converts a string name to a stable uint32 ID using FNV
func hashName(name string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(name))
	return h.Sum32()
}

// ServiceID returns the numeric ID for a given service name.
// Use this on the client side to build requests.
func ServiceID(name string) uint32 { return hashName(name) }
func OperationID(name string) uint32 { return hashName(name) }