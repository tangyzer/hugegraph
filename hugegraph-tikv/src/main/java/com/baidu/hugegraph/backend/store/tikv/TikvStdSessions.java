/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.backend.store.tikv;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.Pair;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;

import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import com.baidu.hugegraph.backend.store.BackendEntryIterator;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.E;
import org.tikv.shade.com.google.protobuf.ByteString;

public class TikvStdSessions extends TikvSessions {

    private final HugeConfig config;

    private volatile RawKVClient tikvClient;

    private final Map<String, Integer> tables;
    private final AtomicInteger refCount;

    private static int tableCode = 0;

    public TikvStdSessions(HugeConfig config, String database, String store) {
        super(config, database, store);
        this.config = config;

        TiConfiguration conf = TiConfiguration.createRawDefault(
                               this.config.get(TikvOptions.TIKV_PDS));
        TiSession session = TiSession.create(conf);
        this.tikvClient = session.createRawClient();

        this.tables = new ConcurrentHashMap<>();
        this.refCount = new AtomicInteger(1);
    }

    @Override
    public void open() throws Exception {
        // pass
    }

    @Override
    protected boolean opened() {
        return this.tikvClient != null;
    }

    @Override
    public Set<String> openedTables() {
        return this.tables.keySet();
    }

    @Override
    public synchronized void createTable(String... tables) {
        for (String table : tables) {
            this.tables.put(table, tableCode++);
        }
    }

    @Override
    public synchronized void dropTable(String... tables) {
        for (String table : tables) {
            this.tables.remove(table);
        }
    }

    @Override
    public boolean existsTable(String table) {
        return this.tables.containsKey(table);
    }

    @Override
    public final Session session() {
        return (Session) super.getOrNewSession();
    }

    @Override
    protected final Session newSession() {
        return new StdSession(this.config());
    }

    @Override
    protected synchronized void doClose() {
        this.checkValid();

        if (this.refCount.decrementAndGet() > 0) {
            return;
        }
        assert this.refCount.get() == 0;

        this.tables.clear();

        this.tikvClient.close();
    }

    private void checkValid() {
    }

    private RawKVClient tikv() {
        this.checkValid();
        return this.tikvClient;
    }

    /**
     * StdSession implement for tikv
     */
    private final class StdSession extends Session {

        private Map<ByteString, ByteString> putBatch;

        public StdSession(HugeConfig conf) {
            this.putBatch = new HashMap<>();
        }

        @Override
        public void open() {
            this.opened = true;
        }

        @Override
        public void close() {
            assert this.closeable();
            this.opened = false;
        }

        @Override
        public boolean closed() {
            return !this.opened;
        }

        @Override
        public void reset() {
            this.putBatch = new HashMap<>();
        }

        /**
         * Any change in the session
         */
        @Override
        public boolean hasChanges() {
            return this.size() > 0;
        }

        /**
         * Commit all updates(put/delete) to DB
         */
        @Override
        public Integer commit() {

            int count = this.size();
            if (count <= 0) {
                return 0;
            }

            if (this.putBatch.size() > 0) {
                tikv().batchPutAtomic(this.putBatch);
                this.putBatch.clear();
            }

            return count;
        }

        /**
         * Rollback all updates(put/delete) not committed
         */
        @Override
        public void rollback() {
            this.putBatch.clear();
        }

        @Override
        public Pair<byte[], byte[]> keyRange(String table) {
            // TODO: implement
            return null;
        }

        /**
         * Add a KV record to a table
         */
        @Override
        public void put(String table, byte[] key, byte[] value) {
            // TODO: implement
        }

        @Override
        public synchronized void increase(String table, byte[] key, byte[] value) {
            // TODO: implement
        }

        @Override
        public void delete(String table, byte[] key) {}

        @Override
        public void deletePrefix(String table, byte[] key) {}

        /**
         * Delete a range of keys from a table
         */
        @Override
        public void deleteRange(String table, byte[] keyFrom, byte[] keyTo) {}

        @Override
        public byte[] get(String table, byte[] key) {
            // TODO: implement
            return null;
        }

        @Override
        public BackendColumnIterator scan(String table) {
            // TODO: implement
            return null;
        }

        @Override
        public BackendColumnIterator scan(String table, byte[] prefix) {
            // TODO: implement
            return null;
        }

        @Override
        public BackendColumnIterator scan(String table, byte[] keyFrom,
                                          byte[] keyTo, int scanType) {
            // TODO: implement
            return null;
        }

        private byte[] b(long value) {
            return ByteBuffer.allocate(Long.BYTES)
                             .order(ByteOrder.nativeOrder())
                             .putLong(value).array();
        }

        private long l(byte[] bytes) {
            assert bytes.length == Long.BYTES;
            return ByteBuffer.wrap(bytes)
                             .order(ByteOrder.nativeOrder())
                             .getLong();
        }

        private int size() {
            return this.putBatch.size();
        }
    }

    private static class ColumnIterator implements BackendColumnIterator,
                                        Countable {

        private final String table;
        private final Iterator<Kvrpcpb.KvPair> iter;

        private final byte[] keyBegin;
        private final byte[] keyEnd;
        private final int scanType;

        private byte[] position;

        public ColumnIterator(String table, List<Kvrpcpb.KvPair> results) {
            this(table, results, null, null, 0);
        }

        public ColumnIterator(String table, List<Kvrpcpb.KvPair> results,
                              byte[] keyBegin, byte[] keyEnd, int scanType) {
            E.checkNotNull(results, "results");
            this.table = table;

            this.iter = results.iterator();
            this.keyBegin = keyBegin;
            this.keyEnd = keyEnd;
            this.scanType = scanType;
        }

        private boolean match(int expected) {
            return Session.matchScanType(expected, this.scanType);
        }

        @Override
        public boolean hasNext() {
            return this.iter.hasNext();
        }

        @Override
        public BackendEntry.BackendColumn next() {
            this.position = this.iter.next().getKey().toByteArray();
            return BackendEntry.BackendColumn.of(this.position,
                                                 this.iter.next().getValue().toByteArray());
        }

        @Override
        public long count() {
            long count = 0L;
            while (this.hasNext()) {
                this.next();
                count++;
                BackendEntryIterator.checkInterrupted();
            }
            return count;
        }

        @Override
        public byte[] position() {
            return this.position;
        }

        @Override
        public void close() {
        }
    }
}
