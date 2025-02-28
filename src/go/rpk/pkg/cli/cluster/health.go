// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cluster

import (
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/types"
	"go.uber.org/zap"
)

func newHealthOverviewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var watch, exit bool
	cmd := &cobra.Command{
		Use:   "health",
		Short: "Queries cluster for health overview",
		Long: `Queries cluster for health overview.

Health overview is created based on the health reports collected periodically
from all nodes in the cluster. A cluster is considered healthy when the
following conditions are met:

  * All cluster nodes are responding
  * All partitions have leaders
  * The cluster controller is present

If the cluster is reported as unhealthy, rpk will exit with code 10.
`,
		Example: `
Basic usage, get cluster health information:
  rpk cluster health

Get cluster health information and watch for changes:
  rpk cluster health --watch

Get cluster health information and exit when the cluster is healthy:
  rpk cluster health --exit-when-healthy
`,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			resp, err := cl.ClusterUUID(cmd.Context())
			var clusterUUID *string
			if err != nil {
				zap.L().Sugar().Warnf("unable to get cluster UUID for the cluster health report: %v; skipping collection and checking cluster health", err)
			} else {
				clusterUUID = &resp.UUID
			}
			// --exit-when-healthy only makes sense with --watch, so we enable
			// watch if --exit-when-healthy is provided.
			watch = exit || watch
			var lastOverview rpadmin.ClusterHealthOverview
			var exit10 bool
			for {
				ret, err := cl.GetHealthOverview(cmd.Context())
				out.MaybeDie(err, "unable to request cluster health: %v", err)
				exit10 = !ret.IsHealthy
				if !reflect.DeepEqual(ret, lastOverview) {
					printHealthOverview(&ret, clusterUUID)
				}
				lastOverview = ret
				if !watch || exit && lastOverview.IsHealthy {
					break
				}
				time.Sleep(2 * time.Second)
			}
			if exit10 {
				// We choose 10 to differentiate from any other error (1), or
				// unhandled panics (2).
				os.Exit(10)
			}
		},
	}
	p.InstallAdminFlags(cmd)
	p.InstallSASLFlags(cmd)

	cmd.Flags().BoolVarP(&watch, "watch", "w", false, "Blocks and writes out all cluster health changes")
	cmd.Flags().BoolVarP(&exit, "exit-when-healthy", "e", false, "Exits when the cluster is back in a healthy state")
	return cmd
}

func printHealthOverview(hov *rpadmin.ClusterHealthOverview, clusterUUID *string) {
	types.Sort(hov)
	out.Section("CLUSTER HEALTH OVERVIEW")

	// leaderless partitions and under-replicated counts are available starting
	// v23.3.
	lp := "Leaderless partitions:"
	urp := "Under-replicated partitions:"
	if hov.LeaderlessCount != nil {
		lp = fmt.Sprintf("Leaderless partitions (%v):", *hov.LeaderlessCount)
		if *hov.LeaderlessCount > len(hov.LeaderlessPartitions) {
			hov.LeaderlessPartitions = append(hov.LeaderlessPartitions, "...truncated")
		}
	}
	if hov.UnderReplicatedCount != nil {
		urp = fmt.Sprintf("Under-replicated partitions (%v):", *hov.UnderReplicatedCount)
		if *hov.UnderReplicatedCount > len(hov.UnderReplicatedPartitions) {
			hov.UnderReplicatedPartitions = append(hov.UnderReplicatedPartitions, "...truncated")
		}
	}

	tw := out.NewTable()
	defer tw.Flush()
	tw.Print("Healthy:", hov.IsHealthy)
	tw.Print("Unhealthy reasons:", hov.UnhealthyReasons)
	tw.Print("Controller ID:", hov.ControllerID)
	tw.Print("All nodes:", hov.AllNodes)
	tw.Print("Nodes down:", hov.NodesDown)
	if hov.NodesInRecoveryMode != nil {
		tw.Print("Nodes in recovery mode:", hov.NodesInRecoveryMode)
	}
	tw.Print(lp, hov.LeaderlessPartitions)
	tw.Print(urp, hov.UnderReplicatedPartitions)
	if clusterUUID != nil {
		tw.Print("Cluster UUID:", *clusterUUID)
	}
}
