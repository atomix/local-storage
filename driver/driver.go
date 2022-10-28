// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"github.com/atomix/runtime/sdk/pkg/network"
	"github.com/atomix/runtime/sdk/pkg/runtime"
)

var driverID = runtime.DriverID{
	Name:    "Memory",
	Version: "v1beta1",
}

func New(network network.Network) runtime.Driver {
	return &sharedMemoryDriver{
		network: network,
	}
}

type sharedMemoryDriver struct {
	network network.Network
}

func (d *sharedMemoryDriver) ID() runtime.DriverID {
	return driverID
}

func (d *sharedMemoryDriver) Connect(ctx context.Context, spec runtime.ConnSpec) (runtime.Conn, error) {
	conn := newConn(d.network)
	if err := conn.Connect(ctx, spec); err != nil {
		return nil, err
	}
	return conn, nil
}

func (d *sharedMemoryDriver) String() string {
	return d.ID().String()
}
