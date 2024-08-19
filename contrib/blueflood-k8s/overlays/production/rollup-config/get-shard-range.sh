#!/bin/bash

[ -z "$ROLLUP_NODE_COUNT" ] && {
  echo "You must set ROLLUP_NODE_COUNT to the number of rollup nodes so that shard"
  echo "distribution can be calculated!"
  exit 1
}

# Total number of shards to assign
shard_count=128
# Shards id's are 0-based, so the max id is 1 less than the count
max_shard_id=$((shard_count - 1))
# Redundancy factor in case of pod downtime. This is how many pods will each shard will be assigned to.
# Setting to 1 will mean if a pod is down, that shard won't be rolled up.
redundancy=2
# Each node will be responsible for a number of shards equal to (shards / nodes * redundancy).
# For example (and remembering that bash truncates on integer division):
# 3 nodes with redundancy 1 == 42 shards/node
# 3 nodes with redundancy 2 == 84 shards/node
# Add one so we always over-allocate, rather than succumbing to rounding errors.
shards_to_assign=$((shard_count / ROLLUP_NODE_COUNT * redundancy + 1))
# Finally, pick the range of shards based on this node's numeric id, assigned because it's in a
# StatefulSet. For easier testing, "hostname" may be passed in.
hostname=${hostname:-$(hostname)}
[[ $hostname =~ -([0-9]+)$ ]] || {
  echo "This machine's hostname doesn't end with '-<digit>'; exiting."
  exit 1
}
node_number=${BASH_REMATCH[1]}
# The ranges always start on simple shard_count/node_count lines.
shard_range_start=$((shard_count / ROLLUP_NODE_COUNT * node_number))
# The end of the range depends on how many shards we should assign
shard_range_end=$((shard_range_start + shards_to_assign))
# Whenever redundancy > 1, the final range will exceed the maximum shard id and should wrap around to
# overlap with the range owned by node 0.
if [ $shard_range_end -ge $max_shard_id ]; then
  second_range_end=$((shard_range_end % max_shard_id))
  shard_range="$(seq -s , $shard_range_start $max_shard_id),$(seq -s , 0 $second_range_end)"
else
  shard_range="$(seq -s , $shard_range_start $shard_range_end)"
fi
echo "$shard_range"
