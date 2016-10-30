package com.rackspacecloud.blueflood.io.serializers.metrics;

import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.rollup.SlotKey;

public class SlotKeySerDes {

    public static SlotKey deserialize(String stateStr) {
        Granularity g = granularityFromSlotKey(stateStr);
        Integer slot = slotFromSlotKey(stateStr);
        int shard = shardFromSlotKey(stateStr);

        return SlotKey.of(g, slot, shard);
    }

    public String serialize(SlotKey slotKey) {
        return serialize(slotKey.getGranularity(), slotKey.getSlot(), slotKey.getShard());
    }

    public String serialize(Granularity gran, int slot, int shard) {
        return new StringBuilder()
                .append(gran == null ? "null" : gran.name())
                .append(",").append(slot)
                .append(",").append(shard)
                .toString();
    }

    protected static Granularity granularityFromSlotKey(String s) {
        String field = s.split(",", -1)[0];
        for (Granularity g : Granularity.granularities())
            if (g.name().startsWith(field))
                return g;
        return null;
    }

    protected static int slotFromSlotKey(String s) {
        return Integer.parseInt(s.split(",", -1)[1]);
    }

    protected static int shardFromSlotKey(String s) {
        return Integer.parseInt(s.split(",", -1)[2]);
    }
}
