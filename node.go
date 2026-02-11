package node

import (
	"github.com/nikitakosatka/hive/pkg/hive"
)

type OrderedNode interface {
	hive.Node

	// DeliveredMessages returns all application-level messages that have been
	// delivered to this node. This is used by tests together with the Orderer
	// to verify both reliability and ordering/parallelism properties.
	DeliveredMessages() []hive.Message
}

// ReliableOrderedNode is a reference implementation of OrderedNode.
type ReliableOrderedNode struct {
	*hive.BaseNode
}

// NewReliableOrderedNode creates a new node with an initial zeroed vector
// clock for all known node IDs.
func NewReliableOrderedNode(id string, allNodeIDs []string) OrderedNode {
	panic("Not implemented")
}

// DeliveredMessages returns the application-level messages that have been
// delivered to this node, in delivery order.
func (n *ReliableOrderedNode) DeliveredMessages() []hive.Message {
	panic("Not implemented")
}

func (n *ReliableOrderedNode) Send(to string, payload interface{}) error {
	// To send message use n.SendMessage()
	panic("Not implemented")
}

func (n *ReliableOrderedNode) Receive(msg *hive.Message) error {
	panic("Not implemented")
}

// Orderer orders events based on their vector clocks and identifies groups of
// mutually parallel events.
type Orderer struct{}

func (o *Orderer) Order(msgs ...hive.Message) (ordered []string, parallel [][]string) {
	panic("Not implemented")
}
