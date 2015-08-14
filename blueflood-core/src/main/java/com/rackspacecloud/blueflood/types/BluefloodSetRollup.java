package com.rackspacecloud.blueflood.types;


import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class BluefloodSetRollup implements Rollup {
    
    private Set<Integer> hashes = new HashSet<Integer>();
    
    public BluefloodSetRollup() {}
    
    public BluefloodSetRollup withObject(Object o) {
        hashes.add(o.hashCode());
        return this;
    }
    
    public static BluefloodSetRollup buildRollupFromSetRollups(Points<BluefloodSetRollup> input) throws IOException {
        BluefloodSetRollup rollup = new BluefloodSetRollup();
        for (Points.Point<BluefloodSetRollup> point : input.getPoints().values()) {
            for (Integer i : point.getData().getHashes()) {
                rollup.hashes.add(i);
            }
        }
        return rollup;
    }
    
    public boolean contains(Object obj) {
        return hashes.contains(obj.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof BluefloodSetRollup)) {
            return false;
        }
        BluefloodSetRollup other = (BluefloodSetRollup)obj;
        return hashes.equals(other.hashes);
    }

    @Override
    public Boolean hasData() {
        return hashes.size() > 0;
    }

    @Override
    public RollupType getRollupType() {
        return RollupType.SET;
    }

    public int getCount() {
        return hashes.size();
    }
    
    public Iterable<Integer> getHashes() {
        return new Iterable<Integer>() {
            @Override
            public Iterator<Integer> iterator() {
                return new ReadOnlyIterator<Integer>(hashes.iterator());
            }
        };
    }
    
    private class ReadOnlyIterator<K> implements Iterator<K> {
        private final Iterator<K> composed;
        public ReadOnlyIterator(Iterator<K> composed) {
            this.composed = composed;
        }

        @Override
        public boolean hasNext() {
            return composed.hasNext();
        }

        @Override
        public K next() {
            return composed.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not allowed");
        }
    }
}
