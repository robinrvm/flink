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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.AbstractCheckpointStats;
import org.apache.flink.runtime.checkpoint.CheckpointStatsCounts;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointStatsStatus;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStats;
import org.apache.flink.runtime.checkpoint.SubtaskStateStats;
import org.apache.flink.runtime.checkpoint.TaskStateStats;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadInfo;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.apache.flink.configuration.RestartStrategyOptions.RestartStrategyType.NO_RESTART_STRATEGY;

/**
 * Integration test for rescaling jobs with mixed (UC-supported and UC-unsupported) exchanges from
 * an unaligned checkpoint.
 */
@RunWith(Parameterized.class)
public class UnalignedCheckpointRescaleWithMixedExchangesITCase extends TestLogger {

    private static final int NUM_TASK_MANAGERS = 1;
    private static final int SLOTS_PER_TASK_MANAGER = 10;
    private static final int MAX_SLOTS = NUM_TASK_MANAGERS * SLOTS_PER_TASK_MANAGER;
    private static final Random RANDOM = new Random();

    // FLINK-40804 (flaky test loop only): test case 3 with fixed parallelism instead of random,
    // set to a combination that often fails (Source 1, Source 2 + Map, Co-Map + Map after
    // forward, Map after keyBy).
    private static final DagParallelism INITIAL_PARALLELISM = new DagParallelism(8, 5, 10, 6);
    private static final DagParallelism RESCALED_PARALLELISM = new DagParallelism(2, 4, 4, 1);

    // FLINK-40804 (flaky test loop only): the first job takes checkpoint 1 itself, this long after
    // all its tasks are RUNNING, instead of on the 1 s timer. This sets how long data flows into
    // the queues before checkpoint 1, and so how much in-flight data it saves.
    private static final long CHECKPOINT_1_DELAY_MS = 900;

    /** The parallelism the next {@link #createMixedComplexityDAG} call uses. */
    private static DagParallelism currentParallelism;

    private static InMemoryReporter reporter;
    private static MiniClusterWithClientResource cluster;
    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Parameterized.Parameter public ExecuteJobViaEnv executeJobViaEnv;

    @Parameterized.Parameters(name = "Test case {index}")
    public static Collection<ExecuteJobViaEnv> parameter() {
        // FLINK-40804 (flaky test loop only): only test case 3
        return Lists.newArrayList(
                UnalignedCheckpointRescaleWithMixedExchangesITCase::createMixedComplexityDAG);
    }

    @Before
    public void setup() throws Exception {
        // MAX_RETAINED_CHECKPOINTS must be set at cluster level because
        // StandaloneCompletedCheckpointStore reads it from the cluster config,
        // not the per-job config. This prevents the picked checkpoint from being
        // subsumed and deleted before the next job restores from it.
        Configuration clusterConfig = new Configuration();
        clusterConfig.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 50);
        // FLINK-40804 (flaky test loop only): metrics for the Sampler; detailed network metrics
        // give the queue length per output (e.g. both outputs of Co-Map)
        reporter = InMemoryReporter.create();
        reporter.addToConfiguration(clusterConfig);
        clusterConfig.set(NettyShuffleEnvironmentOptions.NETWORK_DETAILED_METRICS, true);
        // the TaskManager's buffers are created with this, not with the job configuration
        log.info(
                "F40804-CLUSTER memorySegmentSize={}",
                clusterConfig.get(TaskManagerOptions.MEMORY_SEGMENT_SIZE).toHumanReadableString());
        cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(clusterConfig)
                                .setNumberTaskManagers(NUM_TASK_MANAGERS)
                                .setNumberSlotsPerTaskManager(SLOTS_PER_TASK_MANAGER)
                                .build());
        cluster.before();
    }

    @After
    public void shutDownExistingCluster() {
        if (cluster != null) {
            cluster.after();
            cluster = null;
        }
    }

    /**
     * Tests rescaling from an unaligned checkpoint with different job structures that have mixed
     * (UC-supported and UC-unsupported) exchanges.
     */
    @Test
    public void testRescaleFromUnalignedCheckpoint() throws Exception {
        final MiniCluster miniCluster = cluster.getMiniCluster();

        // Step 1: Run the job with initial parallelism and take a checkpoint
        currentParallelism = INITIAL_PARALLELISM;
        JobClient jobClient1 = executeJobViaEnv.executeJob(getUnalignedCheckpointEnv(null));
        String checkpointPath;
        try (Sampler ignored =
                new Sampler(miniCluster, jobClient1.getJobID(), "INITIAL", currentParallelism)) {
            CommonTestUtils.waitForJobStatus(
                    jobClient1, Collections.singletonList(JobStatus.RUNNING));
            CommonTestUtils.waitForAllTaskRunning(miniCluster, jobClient1.getJobID(), false);
            Thread.sleep(CHECKPOINT_1_DELAY_MS);
            log.info(
                    "F40804-CHECKPOINT1 triggering checkpoint 1 {} ms after all tasks are RUNNING",
                    CHECKPOINT_1_DELAY_MS);
            checkpointPath = miniCluster.triggerCheckpoint(jobClient1.getJobID()).get();
            log.info("F40804-CHECKPOINT1 completed: {}", checkpointPath);
            CommonTestUtils.terminateJob(jobClient1);
        }

        // Step 2: Restore the job with a different parallelism
        currentParallelism = RESCALED_PARALLELISM;
        JobClient jobClient2 =
                executeJobViaEnv.executeJob(getUnalignedCheckpointEnv(checkpointPath));
        try (Sampler ignored =
                new Sampler(miniCluster, jobClient2.getJobID(), "RESCALED", currentParallelism)) {
            CommonTestUtils.waitForJobStatus(
                    jobClient2, Collections.singletonList(JobStatus.RUNNING));
            CommonTestUtils.waitForAllTaskRunning(miniCluster, jobClient2.getJobID(), false);
            CommonTestUtils.waitForCheckpointWithInflightBuffers(
                    jobClient2.getJobID(), miniCluster);
            CommonTestUtils.terminateJob(jobClient2);
        }
    }

    private StreamExecutionEnvironment getUnalignedCheckpointEnv(@Nullable String recoveryPath)
            throws IOException {
        Configuration conf = new Configuration();
        // FLINK-40804 (flaky test loop only): no timer checkpoints in the first job, it takes
        // checkpoint 1 itself (see CHECKPOINT_1_DELAY_MS). The restored job is unchanged.
        conf.set(
                CheckpointingOptions.CHECKPOINTING_INTERVAL,
                recoveryPath == null ? Duration.ofHours(1) : Duration.ofSeconds(1));
        // Disable aligned timeout to ensure it works with unaligned checkpoint directly
        conf.set(CheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT, Duration.ofSeconds(0));
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, NO_RESTART_STRATEGY.getMainValue());
        conf.set(
                CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        conf.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                temporaryFolder.newFolder().toURI().toString());
        conf.set(CheckpointingOptions.ENABLE_UNALIGNED, true);
        // Decrease the memory segment size to avoid the test is so slow for some reasons:
        // 1. When a flink job recovers from unaligned checkpoint, it has to consume all inflight
        // buffers during recovery phase. The smaller the buffer size, the fewer records are
        // snapshotted during the checkpoint, resulting in fewer records are needed to be consumed
        // during recovery.
        // 2. Forward or rescale exchange does not support unaligned checkpoint, it means Forward
        // or rescale exchanges are still using aligned checkpoint even if unaligned checkpoint is
        // enabled. All buffers(records) before barrier must be consumed for aligned checkpoint.
        // The smaller the buffer size means the fewer records are needed to be consumed during
        // aligned checkpoint.
        conf.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, MemorySize.parse("1 kb"));
        if (recoveryPath != null) {
            conf.set(StateRecoveryOptions.SAVEPOINT_PATH, recoveryPath);
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.disableOperatorChaining();
        return env;
    }

    private static JobClient createMultiOutputDAG(StreamExecutionEnvironment env) throws Exception {

        int sourceParallelism = getRandomParallelism();
        DataStream<Long> sourceStream =
                env.fromSource(createSource(), WatermarkStrategy.noWatermarks(), "Data Generator")
                        .setParallelism(sourceParallelism);

        sourceStream
                .keyBy((KeySelector<Long, Long>) value -> value)
                .map(
                        x -> {
                            Thread.sleep(5);
                            return x;
                        })
                .name("Map after keyBy")
                .setParallelism(getRandomParallelism());

        // Keep the same parallelism to ensure the ForwardPartitioner will be used.
        sourceStream
                .map(
                        x -> {
                            Thread.sleep(1);
                            return x;
                        })
                .name("Map after forward")
                .setParallelism(sourceParallelism);

        return env.executeAsync();
    }

    private static JobClient createMultiInputDAG(StreamExecutionEnvironment env) throws Exception {
        int source1Parallelism = getRandomParallelism();
        DataStream<Long> sourceStream1 =
                env.fromSource(createSource(), WatermarkStrategy.noWatermarks(), "Source 1")
                        .setParallelism(source1Parallelism);

        int source2Parallelism = getRandomParallelism();
        DataStream<Long> sourceStream2 =
                env.fromSource(createSource(), WatermarkStrategy.noWatermarks(), "Source 2")
                        .setParallelism(source2Parallelism);

        // Keep the same parallelism to ensure the ForwardPartitioner will be used.
        DataStream<Long> forwardedStream =
                sourceStream2.map(x -> x).setParallelism(source2Parallelism);

        sourceStream1
                .rebalance()
                .connect(forwardedStream.rebalance())
                .map(new SleepingCoMap())
                .name("Co-Map")
                .setParallelism(getRandomParallelism());

        return env.executeAsync();
    }

    private static JobClient createRescalePartitionerDAG(StreamExecutionEnvironment env)
            throws Exception {

        int sourceParallelism = getRandomParallelism();
        DataStream<Long> sourceStream =
                env.fromSource(createSource(), WatermarkStrategy.noWatermarks(), "Data Generator")
                        .setParallelism(sourceParallelism);

        sourceStream
                .keyBy((KeySelector<Long, Long>) value -> value)
                .map(
                        x -> {
                            Thread.sleep(5);
                            return x;
                        })
                .name("Map after keyBy")
                .setParallelism(getRandomParallelism());

        sourceStream
                .rescale()
                .map(
                        x -> {
                            Thread.sleep(1);
                            return x;
                        })
                .name("Map after rescale")
                .setParallelism(getRandomParallelism());

        return env.executeAsync();
    }

    private static JobClient createMixedComplexityDAG(StreamExecutionEnvironment env)
            throws Exception {
        // Multi-input part
        int source1Parallelism = currentParallelism.source1;
        DataStream<Long> sourceStream1 =
                env.fromSource(createSource(), WatermarkStrategy.noWatermarks(), "Source 1")
                        .setParallelism(source1Parallelism);

        int source2Parallelism = currentParallelism.source2;
        DataStream<Long> sourceStream2 =
                env.fromSource(createSource(), WatermarkStrategy.noWatermarks(), "Source 2")
                        .setParallelism(source2Parallelism);

        // Keep the same parallelism to ensure the ForwardPartitioner will be used.
        DataStream<Long> forwardedStream =
                sourceStream2.map(x -> x).setParallelism(source2Parallelism);

        DataStream<Long> multiInputMap =
                sourceStream1
                        .rebalance()
                        .connect(forwardedStream.rebalance())
                        .map(new SleepingCoMap())
                        .name("Co-Map")
                        .setParallelism(currentParallelism.coMap);

        // Multi-output part
        multiInputMap
                .keyBy((KeySelector<Long, Long>) value -> value)
                .map(
                        x -> {
                            Thread.sleep(5);
                            return x;
                        })
                .name("Map after keyBy")
                .setParallelism(currentParallelism.mapAfterKeyBy);

        // Keep the same parallelism to ensure the ForwardPartitioner will be used.
        multiInputMap
                .map(
                        x -> {
                            Thread.sleep(1);
                            return x;
                        })
                .name("Map after forward")
                .setParallelism(multiInputMap.getParallelism());

        return env.executeAsync();
    }

    /**
     * Creates a DAG where the downstream MapAfterKeyBy task receives input from two hash exchanges:
     * one with actual data and one that is empty due to filtering. This tests unaligned checkpoint
     * rescaling with mixed empty and non-empty hash partitions.
     */
    private static JobClient createPartEmptyHashExchangeDAG(StreamExecutionEnvironment env)
            throws Exception {
        int source1Parallelism = getRandomParallelism();
        DataStream<Long> sourceStream1 =
                env.fromSource(createSource(), WatermarkStrategy.noWatermarks(), "Source 1")
                        .setParallelism(source1Parallelism);

        int source2Parallelism = getRandomParallelism();

        // Filter all records to simulate empty state exchange
        DataStream<Long> sourceStream2 =
                env.fromSource(createSource(), WatermarkStrategy.noWatermarks(), "Source 2")
                        .setParallelism(source2Parallelism)
                        .filter(value -> false)
                        .setParallelism(source2Parallelism);

        sourceStream1
                .union(sourceStream2)
                .keyBy((KeySelector<Long, Long>) value -> value)
                .map(
                        x -> {
                            Thread.sleep(5);
                            return x;
                        })
                .name("MapAfterKeyBy")
                .setParallelism(getRandomParallelism());

        return env.executeAsync();
    }

    private static int getRandomParallelism() {
        return RANDOM.nextInt(MAX_SLOTS) + 1;
    }

    private static DataGeneratorSource<Long> createSource() {
        return new DataGeneratorSource<>(
                index -> index,
                new NumberSequenceSource(0, Long.MAX_VALUE - 1) {
                    @Override
                    protected List<NumberSequenceSplit> splitNumberRange(
                            long from, long to, int numSplitsIgnored) {
                        return super.splitNumberRange(from, to, MAX_SLOTS);
                    }
                },
                RateLimiterStrategy.perSecond(5000),
                Types.LONG);
    }

    /** A simple CoMapFunction that sleeps for 1ms for each element. */
    private static class SleepingCoMap implements CoMapFunction<Long, Long, Long> {
        @Override
        public Long map1(Long value) throws Exception {
            Thread.sleep(1);
            return value;
        }

        @Override
        public Long map2(Long value) throws Exception {
            Thread.sleep(1);
            return value;
        }
    }

    public interface ExecuteJobViaEnv {
        JobClient executeJob(StreamExecutionEnvironment env) throws Exception;
    }

    /** FLINK-40804 (flaky test loop only): parallelism of the operators of test case 3. */
    private static final class DagParallelism {
        private final int source1;
        private final int source2;
        private final int coMap;
        private final int mapAfterKeyBy;

        private DagParallelism(int source1, int source2, int coMap, int mapAfterKeyBy) {
            this.source1 = source1;
            this.source2 = source2;
            this.coMap = coMap;
            this.mapAfterKeyBy = mapAfterKeyBy;
        }

        @Override
        public String toString() {
            // Map has the parallelism of Source 2 and Map after forward the one of Co-Map
            return String.format(
                    "Source 1=%d, Source 2=%d, Map=%d, Co-Map=%d, Map after keyBy=%d, Map after forward=%d",
                    source1, source2, source2, coMap, mapAfterKeyBy, coMap);
        }
    }

    /**
     * FLINK-40804 (flaky test loop only): logs once per second until closed.
     *
     * <ul>
     *   <li>F40804-METRICS, per subtask (index from 0): records in/out and the records per second
     *       since the previous line, queue lengths (in buffers) and sizes (in bytes), buffer pool
     *       usage, isBackPressured, and Output.N queue lengths per output. These are read live. Exceptions: the *TimeMsPerSecond values are Flink's 60 s averages,
     *       updated every 5 s, and the accumulate*TimeMs totals move every 5 s.
     *   <li>F40804-CHECKPOINTS and F40804-PENDING: checkpoint counts, and the in-progress
     *       checkpoint with the subtasks that haven't acknowledged it yet.
     *   <li>F40804-JVM: CPU, heap, GC and threads of this JVM, which runs the MiniCluster.
     * </ul>
     *
     * <p>F40804-THREADS: stack traces of the task threads when a checkpoint has been in progress
     * for 60 s, 300 s and 590 s (the checkpoint timeout is 600 s).
     */
    private static final class Sampler implements AutoCloseable {
        private static final Logger LOG = LoggerFactory.getLogger(Sampler.class);

        private static final List<String> TASK_METRICS =
                Arrays.asList(
                        "numRecordsIn",
                        "numRecordsOut",
                        "isBackPressured",
                        "busyTimeMsPerSecond",
                        "backPressuredTimeMsPerSecond",
                        "softBackPressuredTimeMsPerSecond",
                        "hardBackPressuredTimeMsPerSecond",
                        "idleTimeMsPerSecond",
                        "accumulateBusyTimeMs",
                        "accumulateBackPressuredTimeMs",
                        "accumulateIdleTimeMs",
                        "inputQueueLength",
                        "inputQueueSize",
                        "inPoolUsage",
                        "outputQueueLength",
                        "outputQueueSize",
                        "outPoolUsage",
                        "checkpointStartDelayNanos",
                        "checkpointAlignmentTime");

        /**
         * Detailed network metrics per output (Output.N). The per input ones (Input.N) only count
         * remote channels, and this MiniCluster only has local ones, so they are not logged.
         */
        private static final List<String> QUEUE_METRICS =
                Arrays.asList("totalQueueLen", "maxQueueLen");

        private static final long[] THREAD_DUMP_AFTER_MS = {60_000, 300_000, 590_000};

        /** Task thread names, e.g. "Map after keyBy (1/1)#0". */
        private static final Pattern TASK_THREAD_NAME = Pattern.compile(".+ \\(\\d+/\\d+\\)#\\d+");

        private final MiniCluster miniCluster;
        private final JobID jobId;
        private final String phase;
        private final ScheduledExecutorService executor;

        /** Per subtask: numRecordsIn, numRecordsOut and time (ms) of the previous line. */
        private final Map<String, long[]> previousRecords = new HashMap<>();

        private final Set<String> threadDumpsDone = new HashSet<>();

        private Sampler(
                MiniCluster miniCluster, JobID jobId, String phase, DagParallelism parallelism) {
            this.miniCluster = miniCluster;
            this.jobId = jobId;
            this.phase = phase;
            LOG.info("F40804-PHASE phase={} jobId={} parallelism=[{}]", phase, jobId, parallelism);
            this.executor =
                    Executors.newSingleThreadScheduledExecutor(
                            runnable -> {
                                Thread thread = new Thread(runnable, "F40804-sampler-" + phase);
                                thread.setDaemon(true);
                                return thread;
                            });
            executor.scheduleAtFixedRate(this::sample, 0, 1, TimeUnit.SECONDS);
        }

        private void sample() {
            // catch everything: an exception would stop the scheduled sampling
            try {
                logMetrics();
            } catch (Throwable t) {
                LOG.info("F40804-METRICS phase={} not available: {}", phase, t.toString());
            }
            try {
                logCheckpoints();
            } catch (Throwable t) {
                LOG.info("F40804-CHECKPOINTS phase={} not available: {}", phase, t.toString());
            }
            try {
                logJvm();
            } catch (Throwable t) {
                LOG.info("F40804-JVM phase={} not available: {}", phase, t.toString());
            }
        }

        private void logMetrics() {
            Map<String, Map<String, String>> subtasks = new TreeMap<>();
            for (Map.Entry<MetricGroup, Map<String, Metric>> group :
                    reporter.getMetricsByGroup().entrySet()) {
                Map<String, String> variables = group.getKey().getAllVariables();
                String task = variables.get(ScopeFormat.SCOPE_TASK_NAME);
                String subtask = variables.get(ScopeFormat.SCOPE_TASK_SUBTASK_INDEX);
                // task level only, operator metrics use the same names (e.g. numRecordsIn)
                if (task == null
                        || subtask == null
                        || variables.containsKey(ScopeFormat.SCOPE_OPERATOR_NAME)
                        || !jobId.toString().equals(variables.get(ScopeFormat.SCOPE_JOB_ID))) {
                    continue;
                }
                String[] scope = group.getKey().getScopeComponents();
                String prefix = "";
                List<String> names = TASK_METRICS;
                if (scope.length >= 2
                        && scope[scope.length - 1].matches("\\d+")
                        && "Output".equals(scope[scope.length - 2])) {
                    prefix = scope[scope.length - 2] + "." + scope[scope.length - 1] + ".";
                    names = QUEUE_METRICS;
                }
                Map<String, String> values =
                        subtasks.computeIfAbsent(task + " #" + subtask, k -> new TreeMap<>());
                for (String name : names) {
                    Metric metric = group.getValue().get(name);
                    if (metric != null) {
                        values.put(prefix + name, format(metric));
                    }
                }
            }
            long now = System.currentTimeMillis();
            for (Map.Entry<String, Map<String, String>> subtask : subtasks.entrySet()) {
                addRecordsPerSecond(subtask.getKey(), subtask.getValue(), now);
                LOG.info(
                        "F40804-METRICS phase={} subtask=\"{}\" {}",
                        phase,
                        subtask.getKey(),
                        subtask.getValue());
            }
        }

        private void addRecordsPerSecond(String subtask, Map<String, String> values, long now) {
            String in = values.get("numRecordsIn");
            String out = values.get("numRecordsOut");
            if (in == null || out == null) {
                return;
            }
            long[] current = {Long.parseLong(in), Long.parseLong(out), now};
            long[] previous = previousRecords.put(subtask, current);
            if (previous != null && now > previous[2]) {
                double seconds = (now - previous[2]) / 1000.0;
                values.put(
                        "recordsInPerSecondLast",
                        String.format(Locale.ROOT, "%.1f", (current[0] - previous[0]) / seconds));
                values.put(
                        "recordsOutPerSecondLast",
                        String.format(Locale.ROOT, "%.1f", (current[1] - previous[1]) / seconds));
            }
        }

        private void logCheckpoints() throws Exception {
            AccessExecutionGraph graph =
                    miniCluster.getExecutionGraph(jobId).get(5, TimeUnit.SECONDS);
            CheckpointStatsSnapshot snapshot = graph.getCheckpointStatsSnapshot();
            if (snapshot == null) {
                return;
            }
            CheckpointStatsCounts counts = snapshot.getCounts();
            CompletedCheckpointStats latest = snapshot.getHistory().getLatestCompletedCheckpoint();
            LOG.info(
                    "F40804-CHECKPOINTS phase={} jobState={} completed={} failed={} inProgress={} latestCompleted=[{}]",
                    phase,
                    graph.getState(),
                    counts.getNumberOfCompletedCheckpoints(),
                    counts.getNumberOfFailedCheckpoints(),
                    counts.getNumberOfInProgressCheckpoints(),
                    latest == null
                            ? "none"
                            : String.format(
                                    Locale.ROOT,
                                    "id=%d durationMs=%d processedDataBytes=%d persistedDataBytes=%d",
                                    latest.getCheckpointId(),
                                    latest.getEndToEndDuration(),
                                    latest.getProcessedData(),
                                    latest.getPersistedData()));

            long now = System.currentTimeMillis();
            for (AbstractCheckpointStats checkpoint : snapshot.getHistory().getCheckpoints()) {
                if (checkpoint.getStatus() != CheckpointStatsStatus.IN_PROGRESS) {
                    continue;
                }
                long ageMs = now - checkpoint.getTriggerTimestamp();
                StringBuilder notAcknowledged = new StringBuilder();
                for (TaskStateStats task : checkpoint.getAllTaskStateStats()) {
                    AccessExecutionJobVertex vertex = graph.getJobVertex(task.getJobVertexId());
                    String name =
                            vertex == null ? task.getJobVertexId().toString() : vertex.getName();
                    SubtaskStateStats[] subtaskStats = task.getSubtaskStats();
                    for (int i = 0; i < subtaskStats.length; i++) {
                        if (subtaskStats[i] == null) {
                            notAcknowledged
                                    .append(notAcknowledged.length() == 0 ? "" : ", ")
                                    .append(name)
                                    .append(" #")
                                    .append(i);
                        }
                    }
                }
                LOG.info(
                        "F40804-PENDING phase={} checkpointId={} ageMs={} acknowledged={}/{} notAcknowledged=[{}]",
                        phase,
                        checkpoint.getCheckpointId(),
                        ageMs,
                        checkpoint.getNumberOfAcknowledgedSubtasks(),
                        checkpoint.getNumberOfSubtasks(),
                        notAcknowledged);
                for (long dumpAfterMs : THREAD_DUMP_AFTER_MS) {
                    if (ageMs >= dumpAfterMs
                            && threadDumpsDone.add(
                                    checkpoint.getCheckpointId() + "@" + dumpAfterMs)) {
                        dumpTaskThreads(checkpoint.getCheckpointId(), ageMs);
                        break;
                    }
                }
            }
        }

        private void dumpTaskThreads(long checkpointId, long ageMs) {
            StringBuilder dump = new StringBuilder();
            for (ThreadInfo thread :
                    ManagementFactory.getThreadMXBean().dumpAllThreads(false, false)) {
                if (!TASK_THREAD_NAME.matcher(thread.getThreadName()).matches()) {
                    continue;
                }
                dump.append("\n\"")
                        .append(thread.getThreadName())
                        .append("\" ")
                        .append(thread.getThreadState());
                if (thread.getLockName() != null) {
                    dump.append(" on ").append(thread.getLockName());
                }
                StackTraceElement[] stack = thread.getStackTrace();
                for (int i = 0; i < Math.min(stack.length, 25); i++) {
                    dump.append("\n    at ").append(stack[i]);
                }
            }
            LOG.info(
                    "F40804-THREADS phase={} checkpointId={} ageMs={}{}",
                    phase,
                    checkpointId,
                    ageMs,
                    dump);
        }

        private void logJvm() {
            OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
            String cpu = "processCpuLoad=n/a systemCpuLoad=n/a";
            if (os instanceof com.sun.management.OperatingSystemMXBean) {
                com.sun.management.OperatingSystemMXBean sunOs =
                        (com.sun.management.OperatingSystemMXBean) os;
                cpu =
                        String.format(
                                Locale.ROOT,
                                "processCpuLoad=%.3f systemCpuLoad=%.3f",
                                sunOs.getProcessCpuLoad(),
                                sunOs.getSystemCpuLoad());
            }
            MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
            long gcCount = 0;
            long gcTimeMs = 0;
            for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
                gcCount += Math.max(gc.getCollectionCount(), 0);
                gcTimeMs += Math.max(gc.getCollectionTime(), 0);
            }
            LOG.info(
                    "F40804-JVM phase={} {} systemLoadAverage={} availableProcessors={} heapUsedMb={} heapCommittedMb={} heapMaxMb={} gcCount={} gcTimeMs={} threads={}",
                    phase,
                    cpu,
                    String.format(Locale.ROOT, "%.2f", os.getSystemLoadAverage()),
                    os.getAvailableProcessors(),
                    heap.getUsed() >> 20,
                    heap.getCommitted() >> 20,
                    heap.getMax() >> 20,
                    gcCount,
                    gcTimeMs,
                    ManagementFactory.getThreadMXBean().getThreadCount());
        }

        private static String format(Metric metric) {
            if (metric instanceof Counter) {
                return String.valueOf(((Counter) metric).getCount());
            }
            if (metric instanceof Meter) {
                return String.format(Locale.ROOT, "%.1f", ((Meter) metric).getRate());
            }
            if (metric instanceof Gauge) {
                Object value = ((Gauge<?>) metric).getValue();
                return value instanceof Float || value instanceof Double
                        ? String.format(Locale.ROOT, "%.3f", ((Number) value).doubleValue())
                        : String.valueOf(value);
            }
            return metric.getClass().getSimpleName();
        }

        @Override
        public void close() throws InterruptedException {
            executor.shutdownNow();
            executor.awaitTermination(10, TimeUnit.SECONDS);
            LOG.info("F40804-PHASE phase={} jobId={} sampler stopped", phase, jobId);
        }
    }
}
