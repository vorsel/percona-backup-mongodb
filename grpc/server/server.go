package server

import (
	"fmt"
	"sync"
	"time"

	pb "github.com/percona/mongodb-backup/proto/messages"
)

type MessagesServer struct {
	stopChan chan struct{}
	lock     *sync.Mutex
	clients  map[string]*Client
}

type RegisterPayload struct {
	NodeType string `bson:"NodeType"`
}

func NewMessagesServer() *MessagesServer {
	messagesServer := &MessagesServer{
		lock:    &sync.Mutex{},
		clients: make(map[string]*Client),
	}
	return messagesServer
}

func (s *MessagesServer) Clients() map[string]*Client {
	return s.clients
}

// MessagesChat is the method exposed by gRPC to stream messages between the server and agents
func (s *MessagesServer) MessagesChat(stream pb.Messages_MessagesChatServer) error {
	msg, err := s.readMessage(stream)
	if err != nil {
		return err
	}

	client, err := s.registerClient(msg)
	if err != nil {
		r := &pb.ServerMessage{
			Type:    pb.ServerMessage_ERROR,
			Message: []byte(ClientAlreadyExistsError.Error()),
		}
		stream.Send(r)
		return ClientAlreadyExistsError
	}

	if err = client.StartStreamIO(stream); err != nil {
		return err
	}

	for {
		select {
		case <-s.stopChan:
			return nil
		case msg := <-client.InMsgChan():
			if msg == nil {
				s.unregisterClient(client)
				return nil
			}
			s.processInMessage(client, msg)
		}
	}
}

func (s *MessagesServer) processInMessage(client *Client, msg *pb.ClientMessage) {
	client.LastSeen = time.Now()
	switch msg.Type {
	case pb.ClientMessage_REGISTER:
		client.SendMsg(&pb.ServerMessage{Type: pb.ServerMessage_ERROR, Message: []byte("already registered")})
	case pb.ClientMessage_PONG:
	default:
		msgText := fmt.Sprintf("Message type %d is not implemented yet", msg.Type)
		client.SendMsg(&pb.ServerMessage{Type: pb.ServerMessage_ERROR, Message: []byte(msgText)})
	}
}

func (s *MessagesServer) readMessage(stream pb.Messages_MessagesChatServer) (*pb.ClientMessage, error) {
	in, err := stream.Recv()
	if err != nil {
		r := &pb.ServerMessage{
			Type:    pb.ServerMessage_ERROR,
			Message: []byte(err.Error()),
		}
		stream.Send(r)
		return nil, err
	}
	return in, nil
}

func (s *MessagesServer) registerClient(msg *pb.ClientMessage) (*Client, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if msg.ClientID == "" {
		return nil, fmt.Errorf("Invalid client ID (empty)")
	}

	if _, exists := s.clients[msg.ClientID]; exists {
		return nil, ClientAlreadyExistsError
	}

	registerMsg := msg.GetRegisterMsg()
	if registerMsg == nil || registerMsg.NodeType == pb.NodeType_UNDEFINED {
		return nil, fmt.Errorf("Node type in register payload cannot be empty")
	}
	client := NewClient(msg.ClientID, registerMsg.NodeType)
	s.clients[msg.ClientID] = client
	return client, nil
}

func (s *MessagesServer) unregisterClient(client *Client) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, exists := s.clients[client.ID]; !exists {
		return UnknownClientID
	}

	delete(s.clients, client.ID)
	return nil
}
