/*
 *
 * Copyright 2020 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package grpclb

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"
	lbgrpc "google.golang.org/grpc/balancer/grpclb/grpc_lb_v1"
	lbpb "google.golang.org/grpc/balancer/grpclb/grpc_lb_v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/internal/leakcheck"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"
	testpb "google.golang.org/grpc/test/grpc_testing"
)

// remoteBalancerWithFallback is a test load balancer that can send both server
// lists and explicit fallback responses.
type remoteBalancerWithFallback struct {
	sls      chan *lbpb.ServerList
	fallback chan struct{}
	done     chan struct{}
}

func (b *remoteBalancerWithFallback) stop() {
	close(b.done)
}

func (b *remoteBalancerWithFallback) BalanceLoad(stream lbgrpc.LoadBalancer_BalanceLoadServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	initReq := req.GetInitialRequest()
	if initReq.Name != beServerName {
		return status.Errorf(codes.InvalidArgument, "invalid service name: %v", initReq.Name)
	}
	resp := &lbpb.LoadBalanceResponse{
		LoadBalanceResponseType: &lbpb.LoadBalanceResponse_InitialResponse{
			InitialResponse: &lbpb.InitialLoadBalanceResponse{},
		},
	}
	if err := stream.Send(resp); err != nil {
		return err
	}
	for {
		select {
		case sl, ok := <-b.sls:
			if !ok {
				return nil
			}
			resp = &lbpb.LoadBalanceResponse{
				LoadBalanceResponseType: &lbpb.LoadBalanceResponse_ServerList{
					ServerList: sl,
				},
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
		case <-b.fallback:
			resp = &lbpb.LoadBalanceResponse{
				LoadBalanceResponseType: &lbpb.LoadBalanceResponse_FallbackResponse{
					FallbackResponse: &lbpb.FallbackResponse{},
				},
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
		case <-b.done:
			return nil
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

// TestExplicitFallback verifies that a grpclb client enters fallback mode when
// the remote balancer sends an explicit FallbackResponse, and exits fallback
// when a new server list is received.
func TestExplicitFallback(t *testing.T) {
	defer leakcheck.Check(t)

	r, cleanup := manual.GenerateAndRegisterManualResolver()
	defer cleanup()

	// Start an LB-managed backend.
	beLis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to listen for backend: %v", err)
	}
	beIP := beLis.Addr().(*net.TCPAddr).IP
	bePort := beLis.Addr().(*net.TCPAddr).Port
	beLis = newRestartableListener(beLis)
	defer beLis.Close()
	lbBackends := startBackends(beServerName, false, beLis)
	defer stopBackends(lbBackends)

	// Start a standalone fallback backend.
	fallbackLis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to listen for fallback backend: %v", err)
	}
	defer fallbackLis.Close()
	fallbackBEs := startBackends(beServerName, true, fallbackLis)
	defer stopBackends(fallbackBEs)

	// Start the remote balancer (grpclb server).
	lbLis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to listen for load balancer: %v", err)
	}
	lbLis = newRestartableListener(lbLis)
	defer lbLis.Close()

	ls := &remoteBalancerWithFallback{
		sls:      make(chan *lbpb.ServerList, 1),
		fallback: make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
	lb := grpc.NewServer(grpc.Creds(&serverNameCheckCreds{sn: lbServerName}))
	lbgrpc.RegisterLoadBalancerServer(lb, ls)
	go lb.Serve(lbLis)
	defer func() {
		ls.stop()
		lb.Stop()
	}()

	lbAddr := net.JoinHostPort(fakeName, strconv.Itoa(lbLis.Addr().(*net.TCPAddr).Port))

	sl := &lbpb.ServerList{
		Servers: []*lbpb.Server{{
			IpAddress:        beIP,
			Port:             int32(bePort),
			LoadBalanceToken: lbToken,
		}},
	}

	// Pre-load the server list so it's sent immediately when the client connects.
	ls.sls <- sl

	creds := serverNameCheckCreds{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cc, err := grpc.DialContext(ctx, r.Scheme()+":///"+beServerName,
		grpc.WithTransportCredentials(&creds), grpc.WithContextDialer(fakeNameDialer))
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer cc.Close()
	testC := testpb.NewTestServiceClient(cc)

	// Register both the LB address (GRPCLB) and the standalone fallback backend
	// address (Backend) with the resolver.
	r.UpdateState(resolver.State{Addresses: []resolver.Address{
		{Addr: lbAddr, Type: resolver.GRPCLB, ServerName: lbServerName},
		{Addr: fallbackLis.Addr().String(), Type: resolver.Backend},
	}})

	// Verify RPCs are served by the LB-managed backend.
	var p peer.Peer
	var lbBackendUsed bool
	for i := 0; i < 1000; i++ {
		if _, err := testC.EmptyCall(context.Background(), &testpb.Empty{}, grpc.WaitForReady(true), grpc.Peer(&p)); err != nil {
			t.Fatalf("EmptyCall failed: %v", err)
		}
		if p.Addr.(*net.TCPAddr).Port == bePort {
			lbBackendUsed = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !lbBackendUsed {
		t.Fatalf("No RPC sent to LB-managed backend after 1 second")
	}

	// Send an explicit fallback signal from the remote balancer.
	ls.fallback <- struct{}{}

	// Verify RPCs switch to the fallback backend.
	var fallbackUsed bool
	for i := 0; i < 1000; i++ {
		if _, err := testC.EmptyCall(context.Background(), &testpb.Empty{}, grpc.WaitForReady(true), grpc.Peer(&p)); err != nil {
			t.Fatalf("EmptyCall failed: %v", err)
		}
		if p.Addr.String() == fallbackLis.Addr().String() {
			fallbackUsed = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !fallbackUsed {
		t.Fatalf("No RPC sent to fallback backend after explicit fallback signal")
	}

	// Send a new server list; the client should exit fallback.
	ls.sls <- sl
	var lbBackendUsed2 bool
	for i := 0; i < 1000; i++ {
		if _, err := testC.EmptyCall(context.Background(), &testpb.Empty{}, grpc.WaitForReady(true), grpc.Peer(&p)); err != nil {
			t.Fatalf("EmptyCall failed: %v", err)
		}
		if p.Addr.(*net.TCPAddr).Port == bePort {
			lbBackendUsed2 = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !lbBackendUsed2 {
		t.Fatalf("No RPC sent to LB-managed backend after sending new server list")
	}
}
