// SPDX-FileCopyrightText: 2020-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	counterv1 "github.com/atomix/runtime/primitives/pkg/counter/v1"
	countermapv1 "github.com/atomix/runtime/primitives/pkg/countermap/v1"
	electionv1 "github.com/atomix/runtime/primitives/pkg/election/v1"
	indexedmapv1 "github.com/atomix/runtime/primitives/pkg/indexedmap/v1"
	lockv1 "github.com/atomix/runtime/primitives/pkg/lock/v1"
	mapv1 "github.com/atomix/runtime/primitives/pkg/map/v1"
	multimapv1 "github.com/atomix/runtime/primitives/pkg/multimap/v1"
	setv1 "github.com/atomix/runtime/primitives/pkg/set/v1"
	valuev1 "github.com/atomix/runtime/primitives/pkg/value/v1"
	"github.com/atomix/runtime/sdk/pkg/network"
	"github.com/atomix/runtime/sdk/pkg/protocol/node"
	"github.com/atomix/runtime/sdk/pkg/protocol/statemachine"
	"github.com/atomix/shared-memory-storage/node/pkg/sharedmemory"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cmd := &cobra.Command{
		Use: "atomix-shared-memory-node",
		Run: func(cmd *cobra.Command, args []string) {
			configPath, err := cmd.Flags().GetString("config")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}
			host, err := cmd.Flags().GetString("host")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}
			port, err := cmd.Flags().GetInt("port")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}

			config := sharedmemory.Config{}
			configBytes, err := ioutil.ReadFile(configPath)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			if err := yaml.Unmarshal(configBytes, &config); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			registry := statemachine.NewPrimitiveTypeRegistry()
			counterv1.RegisterStateMachine(registry)
			countermapv1.RegisterStateMachine(registry)
			electionv1.RegisterStateMachine(registry)
			indexedmapv1.RegisterStateMachine(registry)
			lockv1.RegisterStateMachine(registry)
			mapv1.RegisterStateMachine(registry)
			multimapv1.RegisterStateMachine(registry)
			setv1.RegisterStateMachine(registry)
			valuev1.RegisterStateMachine(registry)

			var serverOptions []grpc.ServerOption
			if config.Server.ReadBufferSize != nil {
				serverOptions = append(serverOptions, grpc.ReadBufferSize(*config.Server.ReadBufferSize))
			}
			if config.Server.WriteBufferSize != nil {
				serverOptions = append(serverOptions, grpc.WriteBufferSize(*config.Server.WriteBufferSize))
			}
			if config.Server.MaxSendMsgSize != nil {
				serverOptions = append(serverOptions, grpc.MaxSendMsgSize(*config.Server.MaxSendMsgSize))
			}
			if config.Server.MaxRecvMsgSize != nil {
				serverOptions = append(serverOptions, grpc.MaxRecvMsgSize(*config.Server.MaxRecvMsgSize))
			}
			if config.Server.NumStreamWorkers != nil {
				serverOptions = append(serverOptions, grpc.NumStreamWorkers(*config.Server.NumStreamWorkers))
			}
			if config.Server.MaxConcurrentStreams != nil {
				serverOptions = append(serverOptions, grpc.MaxConcurrentStreams(*config.Server.MaxConcurrentStreams))
			}

			node := node.NewNode(
				network.NewNetwork(),
				sharedmemory.NewProtocol(),
				node.WithHost(host),
				node.WithPort(port),
				node.WithGRPCServerOptions(serverOptions...))

			counterv1.RegisterServer(node)
			countermapv1.RegisterServer(node)
			electionv1.RegisterServer(node)
			indexedmapv1.RegisterServer(node)
			lockv1.RegisterServer(node)
			mapv1.RegisterServer(node)
			multimapv1.RegisterServer(node)
			setv1.RegisterServer(node)
			valuev1.RegisterServer(node)

			// Start the node
			if err := node.Start(); err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}

			// Wait for an interrupt signal
			ch := make(chan os.Signal, 2)
			signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
			<-ch

			// Stop the node
			if err := node.Stop(); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
	}
	cmd.Flags().StringP("config", "c", "", "the path to the node configuration")
	cmd.Flags().String("host", "", "the host to which to bind the server")
	cmd.Flags().Int("port", 8080, "the port to which to bind the server")

	_ = cmd.MarkFlagRequired("node")
	_ = cmd.MarkFlagRequired("config")
	_ = cmd.MarkFlagFilename("config")

	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}
