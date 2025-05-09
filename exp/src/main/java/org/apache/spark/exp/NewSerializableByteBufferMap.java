package org.apache.spark.exp;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ByteBuffers;

class NewSerializableByteBufferMap implements Map<Integer, ByteBuffer>, Serializable {
    private final Map<Integer, ByteBuffer> wrapped;
    private transient volatile Map<Integer, ByteBuffer> immutableMap;

    static Map<Integer, ByteBuffer> wrap(Map<Integer, ByteBuffer> map) {
        if (map == null) {
            return null;
        } else {
            return (Map<Integer, ByteBuffer>)(map instanceof org.apache.spark.exp.NewSerializableByteBufferMap ? map : new org.apache.spark.exp.NewSerializableByteBufferMap(map));
        }
    }

    NewSerializableByteBufferMap() {
        this.wrapped = Maps.newLinkedHashMap();
    }

    private NewSerializableByteBufferMap(Map<Integer, ByteBuffer> wrapped) {
        this.wrapped = wrapped;
    }

    Object writeReplace() throws ObjectStreamException {
        Collection<Map.Entry<Integer, ByteBuffer>> entries = this.wrapped.entrySet();
        int[] keys = new int[entries.size()];
        byte[][] values = new byte[keys.length][];
        int keyIndex = 0;

        for(Map.Entry<Integer, ByteBuffer> entry : entries) {
            keys[keyIndex] = (Integer)entry.getKey();
            values[keyIndex] = ByteBuffers.toByteArray((ByteBuffer)entry.getValue());
            ++keyIndex;
        }

        return new org.apache.spark.exp.NewSerializableByteBufferMap.MapSerializationProxy(keys, values);
    }

    public Map<Integer, ByteBuffer> immutableMap() {
        if (this.immutableMap == null) {
            synchronized(this) {
                if (this.immutableMap == null) {
                    this.immutableMap = Collections.unmodifiableMap(this.wrapped);
                }
            }
        }

        return this.immutableMap;
    }

    public int size() {
        return this.wrapped.size();
    }

    public boolean isEmpty() {
        return this.wrapped.isEmpty();
    }

    public boolean containsKey(Object key) {
        return this.wrapped.containsKey(key);
    }

    public boolean containsValue(Object value) {
        return this.wrapped.containsValue(value);
    }

    public ByteBuffer get(Object key) {
        return (ByteBuffer)this.wrapped.get(key);
    }

    public ByteBuffer put(Integer key, ByteBuffer value) {
        return (ByteBuffer)this.wrapped.put(key, value);
    }

    public ByteBuffer remove(Object key) {
        return (ByteBuffer)this.wrapped.remove(key);
    }

    public void putAll(Map<? extends Integer, ? extends ByteBuffer> m) {
        this.wrapped.putAll(m);
    }

    public void clear() {
        this.wrapped.clear();
    }

    public Set<Integer> keySet() {
        return this.wrapped.keySet();
    }

    public Collection<ByteBuffer> values() {
        return this.wrapped.values();
    }

    public Set<Map.Entry<Integer, ByteBuffer>> entrySet() {
        return this.wrapped.entrySet();
    }

    public boolean equals(Object o) {
        return this.wrapped.equals(o);
    }

    public int hashCode() {
        return this.wrapped.hashCode();
    }

    private static class MapSerializationProxy implements Serializable {
        private int[] keys = null;
        private byte[][] values = null;

        MapSerializationProxy() {
        }

        MapSerializationProxy(int[] keys, byte[][] values) {
            this.keys = keys;
            this.values = values;
        }

        Object readResolve() throws ObjectStreamException {
            Map<Integer, ByteBuffer> map = Maps.newLinkedHashMap();

            for(int i = 0; i < this.keys.length; ++i) {
                map.put(this.keys[i], ByteBuffer.wrap(this.values[i]));
            }

            return org.apache.spark.exp.NewSerializableByteBufferMap.wrap(map);
        }
    }
}

