/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.memory;

import org.apache.flink.core.memory.MemorySegment;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Validate memory release under concurrent modification exceptions. */
class MemoryManagerConcurrentModReleaseTest {

    /**
     * release() retries until one pass over the list sees no concurrent modification. A modifier
     * that never stops can therefore keep release() busy forever, so the modifier stops after
     * release() has started this many passes.
     */
    private static final int MAX_RELEASE_ATTEMPTS_WHILE_MODIFYING = 10;

    @Test
    void testConcurrentModificationOnce() throws MemoryAllocationException {
        final int numSegments = 10000;
        final int segmentSize = 4096;

        MemoryManager memMan =
                MemoryManagerBuilder.newBuilder()
                        .setMemorySize(numSegments * segmentSize)
                        .setPageSize(segmentSize)
                        .build();

        ArrayList<MemorySegment> segs = new ListWithConcModExceptionOnFirstAccess<>();
        memMan.allocatePages(this, segs, numSegments);

        memMan.release(segs);
    }

    @Test
    void testConcurrentModificationWhileReleasing() throws Exception {
        final int numSegments = 10000;
        final int segmentSize = 4096;

        MemoryManager memMan =
                MemoryManagerBuilder.newBuilder()
                        .setMemorySize(numSegments * segmentSize)
                        .setPageSize(segmentSize)
                        .build();

        ReleaseAttemptCountingList<MemorySegment> segs =
                new ReleaseAttemptCountingList<>(numSegments);
        memMan.allocatePages(this, segs, numSegments);

        // start a thread that performs concurrent modifications
        Modifier mod = new Modifier(segs, MAX_RELEASE_ATTEMPTS_WHILE_MODIFYING);
        Thread modRunner = new Thread(mod);
        modRunner.start();
        mod.awaitFirstModification();

        try {
            memMan.release(segs);
        } finally {
            mod.cancel();
        }

        modRunner.join();
        assertThat(segs).isEmpty();
        assertThat(memMan.verifyEmpty()).isTrue();
    }

    private static class Modifier implements Runnable {

        private final ReleaseAttemptCountingList<MemorySegment> toModify;
        private final int maxReleaseAttempts;
        private final CountDownLatch firstModification = new CountDownLatch(1);

        private volatile boolean running = true;

        private Modifier(
                ReleaseAttemptCountingList<MemorySegment> toModify, int maxReleaseAttempts) {
            this.toModify = toModify;
            this.maxReleaseAttempts = maxReleaseAttempts;
        }

        public void cancel() {
            running = false;
        }

        void awaitFirstModification() throws InterruptedException {
            firstModification.await();
        }

        @Override
        public void run() {
            while (running && toModify.getIteratorCalls() < maxReleaseAttempts) {
                try {
                    MemorySegment seg = toModify.remove(0);
                    toModify.add(seg);
                } catch (IndexOutOfBoundsException e) {
                    // may happen, just retry
                }
                firstModification.countDown();
            }
        }
    }

    private static class ReleaseAttemptCountingList<E> extends ArrayList<E> {

        private static final long serialVersionUID = 1L;

        private final AtomicInteger iteratorCalls = new AtomicInteger();

        private ReleaseAttemptCountingList(int initialCapacity) {
            super(initialCapacity);
        }

        int getIteratorCalls() {
            return iteratorCalls.get();
        }

        @Override
        public Iterator<E> iterator() {
            iteratorCalls.incrementAndGet();
            return super.iterator();
        }
    }

    private class ListWithConcModExceptionOnFirstAccess<E> extends ArrayList<E> {

        private static final long serialVersionUID = -1623249699823349781L;

        private boolean returnedIterator;

        @Override
        public Iterator<E> iterator() {
            if (returnedIterator) {
                return super.iterator();
            } else {
                returnedIterator = true;
                return new ConcFailingIterator<>();
            }
        }
    }

    private static class ConcFailingIterator<E> implements Iterator<E> {

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public E next() {
            throw new ConcurrentModificationException();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
