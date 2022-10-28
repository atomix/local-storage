// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package v1beta1

import (
	"github.com/atomix/runtime/sdk/pkg/logging"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

var log = logging.GetLogger()

// AddControllers adds consensus controllers to the manager
func AddControllers(mgr manager.Manager) error {
	if err := addConsensusStoreController(mgr); err != nil {
		return err
	}
	if err := addMultiRaftClusterController(mgr); err != nil {
		return err
	}
	if err := addPodController(mgr); err != nil {
		return err
	}
	return nil
}
