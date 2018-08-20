// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/percona/mongodb-backup/proto/api (interfaces: Api_GetClientsServer)

// Package mock_api is a generated GoMock package.
package mock_api

import (
	context "context"
	gomock "github.com/golang/mock/gomock"
	api "github.com/percona/mongodb-backup/proto/api"
	metadata "google.golang.org/grpc/metadata"
	reflect "reflect"
)

// MockApi_GetClientsServer is a mock of Api_GetClientsServer interface
type MockApi_GetClientsServer struct {
	ctrl     *gomock.Controller
	recorder *MockApi_GetClientsServerMockRecorder
}

// MockApi_GetClientsServerMockRecorder is the mock recorder for MockApi_GetClientsServer
type MockApi_GetClientsServerMockRecorder struct {
	mock *MockApi_GetClientsServer
}

// NewMockApi_GetClientsServer creates a new mock instance
func NewMockApi_GetClientsServer(ctrl *gomock.Controller) *MockApi_GetClientsServer {
	mock := &MockApi_GetClientsServer{ctrl: ctrl}
	mock.recorder = &MockApi_GetClientsServerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockApi_GetClientsServer) EXPECT() *MockApi_GetClientsServerMockRecorder {
	return m.recorder
}

// Context mocks base method
func (m *MockApi_GetClientsServer) Context() context.Context {
	ret := m.ctrl.Call(m, "Context")
	ret0, _ := ret[0].(context.Context)
	return ret0
}

// Context indicates an expected call of Context
func (mr *MockApi_GetClientsServerMockRecorder) Context() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Context", reflect.TypeOf((*MockApi_GetClientsServer)(nil).Context))
}

// RecvMsg mocks base method
func (m *MockApi_GetClientsServer) RecvMsg(arg0 interface{}) error {
	ret := m.ctrl.Call(m, "RecvMsg", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// RecvMsg indicates an expected call of RecvMsg
func (mr *MockApi_GetClientsServerMockRecorder) RecvMsg(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RecvMsg", reflect.TypeOf((*MockApi_GetClientsServer)(nil).RecvMsg), arg0)
}

// Send mocks base method
func (m *MockApi_GetClientsServer) Send(arg0 *api.Client) error {
	ret := m.ctrl.Call(m, "Send", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Send indicates an expected call of Send
func (mr *MockApi_GetClientsServerMockRecorder) Send(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Send", reflect.TypeOf((*MockApi_GetClientsServer)(nil).Send), arg0)
}

// SendHeader mocks base method
func (m *MockApi_GetClientsServer) SendHeader(arg0 metadata.MD) error {
	ret := m.ctrl.Call(m, "SendHeader", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// SendHeader indicates an expected call of SendHeader
func (mr *MockApi_GetClientsServerMockRecorder) SendHeader(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SendHeader", reflect.TypeOf((*MockApi_GetClientsServer)(nil).SendHeader), arg0)
}

// SendMsg mocks base method
func (m *MockApi_GetClientsServer) SendMsg(arg0 interface{}) error {
	ret := m.ctrl.Call(m, "SendMsg", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// SendMsg indicates an expected call of SendMsg
func (mr *MockApi_GetClientsServerMockRecorder) SendMsg(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SendMsg", reflect.TypeOf((*MockApi_GetClientsServer)(nil).SendMsg), arg0)
}

// SetHeader mocks base method
func (m *MockApi_GetClientsServer) SetHeader(arg0 metadata.MD) error {
	ret := m.ctrl.Call(m, "SetHeader", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetHeader indicates an expected call of SetHeader
func (mr *MockApi_GetClientsServerMockRecorder) SetHeader(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetHeader", reflect.TypeOf((*MockApi_GetClientsServer)(nil).SetHeader), arg0)
}

// SetTrailer mocks base method
func (m *MockApi_GetClientsServer) SetTrailer(arg0 metadata.MD) {
	m.ctrl.Call(m, "SetTrailer", arg0)
}

// SetTrailer indicates an expected call of SetTrailer
func (mr *MockApi_GetClientsServerMockRecorder) SetTrailer(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetTrailer", reflect.TypeOf((*MockApi_GetClientsServer)(nil).SetTrailer), arg0)
}
