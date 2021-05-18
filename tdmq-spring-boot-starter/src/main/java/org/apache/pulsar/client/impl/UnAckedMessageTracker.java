//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.pulsar.client.impl;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashSet;
import org.apache.pulsar.shade.com.google.common.base.Preconditions;
import org.apache.pulsar.shade.io.netty.util.Timeout;
import org.apache.pulsar.shade.io.netty.util.TimerTask;
import org.apache.pulsar.shade.io.netty.util.concurrent.FastThreadLocal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnAckedMessageTracker implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(UnAckedMessageTracker.class);
    protected final ConcurrentHashMap<MessageId, ConcurrentOpenHashSet<MessageId>> messageIdPartitionMap;
    protected final ArrayDeque<ConcurrentOpenHashSet<MessageId>> timePartitions;
    protected final Lock readLock;
    protected final Lock writeLock;
    public static final UnAckedMessageTrackerDisabled UNACKED_MESSAGE_TRACKER_DISABLED = new UnAckedMessageTrackerDisabled();
    private final long ackTimeoutMillis;
    private final long tickDurationInMs;
    private Timeout timeout;
    private static final FastThreadLocal<HashSet<MessageId>> TL_MESSAGE_IDS_SET = new FastThreadLocal<HashSet<MessageId>>() {
        protected HashSet<MessageId> initialValue() throws Exception {
            return new HashSet<>();
        }
    };

    public UnAckedMessageTracker() {
        this.readLock = null;
        this.writeLock = null;
        this.timePartitions = null;
        this.messageIdPartitionMap = null;
        this.ackTimeoutMillis = 0L;
        this.tickDurationInMs = 0L;
    }

    public UnAckedMessageTracker(PulsarClientImpl client, ConsumerBase<?> consumerBase, long ackTimeoutMillis) {
        this(client, consumerBase, ackTimeoutMillis, ackTimeoutMillis);
    }

    public UnAckedMessageTracker(final PulsarClientImpl client, final ConsumerBase<?> consumerBase, long ackTimeoutMillis, final long tickDurationInMs) {
        Preconditions.checkArgument(tickDurationInMs > 0L && ackTimeoutMillis >= tickDurationInMs);
        this.ackTimeoutMillis = ackTimeoutMillis;
        this.tickDurationInMs = tickDurationInMs;
        ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        this.readLock = readWriteLock.readLock();
        this.writeLock = readWriteLock.writeLock();
        this.messageIdPartitionMap = new ConcurrentHashMap();
        this.timePartitions = new ArrayDeque();
        int blankPartitions = (int)Math.ceil((double)this.ackTimeoutMillis / (double)this.tickDurationInMs);

        for(int i = 0; i < blankPartitions + 1; ++i) {
            this.timePartitions.add(new ConcurrentOpenHashSet(16, 1));
        }

        this.timeout = client.timer().newTimeout(new TimerTask() {
            public void run(Timeout t) throws Exception {
                Set<MessageId> messageIds = (Set)UnAckedMessageTracker.TL_MESSAGE_IDS_SET.get();
                messageIds.clear();
                UnAckedMessageTracker.this.writeLock.lock();

                try {
                    ConcurrentOpenHashSet<MessageId> headPartition = (ConcurrentOpenHashSet)UnAckedMessageTracker.this.timePartitions.removeFirst();
                    if (!headPartition.isEmpty()) {
                        UnAckedMessageTracker.log.warn("[{}] {} messages have timed-out", consumerBase, headPartition.size());
                        headPartition.forEach((messageId) -> {
                            UnAckedMessageTracker.addChunkedMessageIdsAndRemoveFromSequnceMap(messageId, messageIds, consumerBase);
                            messageIds.add(messageId);
                            UnAckedMessageTracker.this.messageIdPartitionMap.remove(messageId);
                        });
                    }

                    headPartition.clear();
                    UnAckedMessageTracker.this.timePartitions.addLast(headPartition);
                } finally {
                    try {
                        if (messageIds.size() > 0) {
                            consumerBase.onAckTimeoutSend(messageIds);
                            consumerBase.redeliverUnacknowledgedMessages(messageIds);
                        }

                        UnAckedMessageTracker.this.timeout = client.timer().newTimeout(this, tickDurationInMs, TimeUnit.MILLISECONDS);
                    } finally {
                        UnAckedMessageTracker.this.writeLock.unlock();
                    }
                }

            }
        }, this.tickDurationInMs, TimeUnit.MILLISECONDS);
    }

    public static void addChunkedMessageIdsAndRemoveFromSequnceMap(MessageId messageId, Set<MessageId> messageIds, ConsumerBase<?> consumerBase) {
        if (messageId instanceof MessageIdImpl) {
            MessageIdImpl[] chunkedMsgIds = (MessageIdImpl[])consumerBase.unAckedChunckedMessageIdSequenceMap.get((MessageIdImpl)messageId);
            if (chunkedMsgIds != null && chunkedMsgIds.length > 0) {
                MessageIdImpl[] var7 = chunkedMsgIds;
                int var6 = chunkedMsgIds.length;

                for(int var5 = 0; var5 < var6; ++var5) {
                    MessageIdImpl msgId = var7[var5];
                    messageIds.add(msgId);
                }
            }

            consumerBase.unAckedChunckedMessageIdSequenceMap.remove((MessageIdImpl)messageId);
        }

    }

    public void clear() {
        this.writeLock.lock();

        try {
            this.messageIdPartitionMap.clear();
            this.timePartitions.forEach((tp) -> {
                tp.clear();
            });
        } finally {
            this.writeLock.unlock();
        }

    }

    public boolean add(MessageId messageId) {
        this.writeLock.lock();

        boolean var5;
        try {
            ConcurrentOpenHashSet<MessageId> partition = (ConcurrentOpenHashSet)this.timePartitions.peekLast();
            ConcurrentOpenHashSet<MessageId> previousPartition = (ConcurrentOpenHashSet)this.messageIdPartitionMap.putIfAbsent(messageId, partition);
            if (previousPartition != null) {
                return false;
            }

            var5 = partition.add(messageId);
        } finally {
            this.writeLock.unlock();
        }

        return var5;
    }

    boolean isEmpty() {
        this.readLock.lock();

        boolean var2;
        try {
            var2 = this.messageIdPartitionMap.isEmpty();
        } finally {
            this.readLock.unlock();
        }

        return var2;
    }

    public boolean remove(MessageId messageId) {
        this.writeLock.lock();

        boolean var5;
        try {
            boolean removed = false;
            ConcurrentOpenHashSet<MessageId> exist = (ConcurrentOpenHashSet)this.messageIdPartitionMap.remove(messageId);
            if (exist != null) {
                removed = exist.remove(messageId);
            }

            var5 = removed;
        } finally {
            this.writeLock.unlock();
        }

        return var5;
    }

    long size() {
        this.readLock.lock();

        long var2;
        try {
            var2 = (long)this.messageIdPartitionMap.size();
        } finally {
            this.readLock.unlock();
        }

        return var2;
    }

    public int removeMessagesTill(MessageId msgId) {
        this.writeLock.lock();

        int var7;
        try {
            int removed = 0;
            Iterator iterator = this.messageIdPartitionMap.keySet().iterator();

            while(iterator.hasNext()) {
                MessageId messageId = (MessageId)iterator.next();
                if (messageId.compareTo(msgId) <= 0) {
                    ConcurrentOpenHashSet<MessageId> exist = (ConcurrentOpenHashSet)this.messageIdPartitionMap.get(messageId);
                    if (exist != null) {
                        exist.remove(messageId);
                    }

                    iterator.remove();
                    ++removed;
                }
            }

            var7 = removed;
        } finally {
            this.writeLock.unlock();
        }

        return var7;
    }

    private void stop() {
        this.writeLock.lock();

        try {
            if (this.timeout != null && !this.timeout.isCancelled()) {
                this.timeout.cancel();
            }

            this.clear();
        } finally {
            this.writeLock.unlock();
        }

    }

    public void close() {
        this.stop();
    }

    private static class UnAckedMessageTrackerDisabled extends UnAckedMessageTracker {
        private UnAckedMessageTrackerDisabled() {
        }

        public void clear() {
        }

        long size() {
            return 0L;
        }

        public boolean add(MessageId m) {
            return true;
        }

        public boolean remove(MessageId m) {
            return true;
        }

        public int removeMessagesTill(MessageId msgId) {
            return 0;
        }

        public void close() {
        }
    }
}
