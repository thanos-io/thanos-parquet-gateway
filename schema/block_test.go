// Copyright (c) The Thanos Authors.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

package schema

import (
	"maps"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos-parquet-gateway/internal/util"
)

func TestBlockNameForDay(t *testing.T) {
	t.Run("", func(t *testing.T) {
		b := BlockNameForDay(util.NewDate(1970, time.January, 1))
		want := "1970/01/01"
		got := b
		if want != got {
			t.Fatalf("wanted %q, got %q", want, got)
		}
	})
	t.Run("", func(t *testing.T) {
		b := BlockNameForDay(util.NewDate(2024, time.November, 23))
		want := "2024/11/23"
		got := b
		if want != got {
			t.Fatalf("wanted %q, got %q", want, got)
		}
	})
}

func TestExternalLabelsWithout(t *testing.T) {
	t.Run("empty exclude returns copy", func(t *testing.T) {
		in := ExternalLabels{"a": "1", "b": "2"}
		out := in.Without(nil)
		require.True(t, maps.Equal(in, out), "expected equal maps")
		out["c"] = "3"
		require.Len(t, in, 2, "original unchanged")
		out = in.Without([]string{})
		require.True(t, maps.Equal(in, out))
	})
	t.Run("exclude one label removes it", func(t *testing.T) {
		in := ExternalLabels{"cluster": "eu", "receive_node_hostname": "node-a"}
		out := in.Without([]string{"receive_node_hostname"})
		require.Equal(t, ExternalLabels{"cluster": "eu"}, out)
		require.Equal(t, ExternalLabels{"cluster": "eu", "receive_node_hostname": "node-a"}, in, "original unchanged")
	})
	t.Run("exclude multiple labels", func(t *testing.T) {
		in := ExternalLabels{"a": "1", "b": "2", "c": "3"}
		out := in.Without([]string{"b"})
		require.Equal(t, ExternalLabels{"a": "1", "c": "3"}, out)
		out = in.Without([]string{"a", "c"})
		require.Equal(t, ExternalLabels{"b": "2"}, out)
	})
	t.Run("exclude non-existent key is fine", func(t *testing.T) {
		in := ExternalLabels{"a": "1"}
		out := in.Without([]string{"missing"})
		require.True(t, maps.Equal(in, out))
	})
	t.Run("hash of Without(replica) is stable for same non-replica labels", func(t *testing.T) {
		withReplicaA := ExternalLabels{"cluster": "eu", "receive_node_hostname": "node-a"}
		withReplicaB := ExternalLabels{"cluster": "eu", "receive_node_hostname": "node-b"}
		withoutReplica := ExternalLabels{"cluster": "eu"}
		streamA := withReplicaA.Without([]string{"receive_node_hostname"})
		streamB := withReplicaB.Without([]string{"receive_node_hostname"})
		require.True(t, maps.Equal(streamA, withoutReplica))
		require.True(t, maps.Equal(streamB, withoutReplica))
		require.Equal(t, streamA.Hash(), streamB.Hash(), "same stream hash for different replicas")
		require.NotEqual(t, withReplicaA.Hash(), withReplicaB.Hash(), "raw labels differ by replica")
	})
}
