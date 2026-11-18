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

package org.apache.flink.table.planner.plan.stream.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.operations.materializedtable.CreateMaterializedTableOperation;
import org.apache.flink.table.planner.plan.nodes.exec.stream.ProcessTableFunctionTestUtils.UpdatingUpsertFunction;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for EXPLAIN statements. */
public class ExplainTest extends TableTestBase {

    /**
     * When the {@code PLAN_TEST_FORCE_OVERWRITE} environment variable is set {@link #verifyExplain}
     * overwrites the golden files.
     */
    private static final boolean REGENERATE_FILES =
            "true".equalsIgnoreCase(System.getenv("PLAN_TEST_FORCE_OVERWRITE"));

    /** A regular join with an aggregate input, so the join emits update changes. */
    private static final String JOIN_WITH_AGGREGATE_QUERY =
            "SELECT o.a, o.b, t.total FROM MyTable o "
                    + "JOIN (SELECT a, SUM(b) AS total FROM MyTable GROUP BY a) t ON o.a = t.a";

    private StreamTableTestUtil util;
    private TestInfo testInfo;

    @BeforeEach
    void setup(TestInfo testInfo) {
        this.testInfo = testInfo;
        this.util = streamTestUtil(TableConfig.getDefault());
        this.util
                .getTableEnv()
                .executeSql(
                        "CREATE TABLE MyTable (\n"
                                + "  a INT,\n"
                                + "  b BIGINT,\n"
                                + "  c STRING\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values'\n"
                                + ")");
    }

    @Test
    void testExplainCreateMaterializedTable() {
        verifyExplain(
                "CREATE MATERIALIZED TABLE MyMTTable\n"
                        + " WITH (\n"
                        + "   'connector' = 'values'\n"
                        + ") AS\n"
                        + "  SELECT\n"
                        + "    `a`,\n"
                        + "    `b`\n"
                        + "  FROM\n"
                        + "    MyTable");
    }

    @Test
    void testExplainCreateOrAlterMaterializedTable() {
        verifyExplain(
                "CREATE OR ALTER MATERIALIZED TABLE MyMTTable (\n"
                        + " `b`,\n"
                        + " `a`\n"
                        + " )\n"
                        + " WITH (\n"
                        + "   'connector' = 'values'\n"
                        + ") AS\n"
                        + "  SELECT\n"
                        + "    CAST(`a` AS BIGINT) AS `a`,\n"
                        + "    `b`\n"
                        + "  FROM\n"
                        + "    MyTable");
    }

    @Test
    void testExplainAlterMaterializedTable() {
        createMaterializedTableInCatalog(
                "CREATE OR ALTER MATERIALIZED TABLE MyMTTable\n"
                        + " WITH (\n"
                        + "   'connector' = 'values'\n"
                        + ") AS\n"
                        + "  SELECT\n"
                        + "    `a`,\n"
                        + "    `b`\n"
                        + "  FROM\n"
                        + "    MyTable");
        verifyExplain(
                "ALTER MATERIALIZED TABLE MyMTTable\n"
                        + "AS\n"
                        + "  SELECT\n"
                        + "    `a`,\n"
                        + "    `b`,\n"
                        + "    `c`\n"
                        + "  FROM\n"
                        + "    MyTable");
    }

    @Test
    void testExplainFullAlterMaterializedTable() {
        createMaterializedTableInCatalog(
                "CREATE OR ALTER MATERIALIZED TABLE MyMTTable\n"
                        + " WITH (\n"
                        + "   'connector' = 'values'\n"
                        + ") AS\n"
                        + "  SELECT\n"
                        + "    `a`,\n"
                        + "    `b`\n"
                        + "  FROM\n"
                        + "    MyTable");
        verifyExplain(
                "CREATE OR ALTER MATERIALIZED TABLE MyMTTable(\n"
                        + " `b`,\n"
                        + " `a`,\n"
                        + " `c`\n"
                        + " )\n"
                        + " WITH (\n"
                        + "   'connector' = 'values'\n"
                        + ")\n"
                        + "AS\n"
                        + "  SELECT\n"
                        + "    CAST(`a` AS BIGINT) AS `a`,\n"
                        + "    `b`,\n"
                        + "    `c`\n"
                        + "  FROM\n"
                        + "    MyTable");
    }

    @Test
    void testExplainConvertTableToMaterializedTable() {
        final Configuration rootConfiguration = new Configuration();
        rootConfiguration.set(
                TableConfigOptions.MATERIALIZED_TABLE_CONVERSION_FROM_TABLE_ENABLED, true);
        util.getTableEnv().getConfig().setRootConfiguration(rootConfiguration);
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE MyConvertTable (\n"
                                + "  `a` INT,\n"
                                + "  `b` BIGINT\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values'\n"
                                + ")");
        verifyExplain(
                "CREATE OR ALTER MATERIALIZED TABLE MyConvertTable\n"
                        + " AS\n"
                        + "  SELECT\n"
                        + "    `a`,\n"
                        + "    `b`\n"
                        + "  FROM\n"
                        + "    MyTable");
    }

    @Test
    void testExplainCreateTableAsSelect() {
        verifyExplain(
                "CREATE TABLE MyCtasTable\n"
                        + " WITH (\n"
                        + "   'connector' = 'values'\n"
                        + ") AS\n"
                        + "  SELECT\n"
                        + "    `a`,\n"
                        + "    `b`\n"
                        + "  FROM\n"
                        + "    MyTable",
                "testExplainCtas");
    }

    @Test
    void testExplainReplaceTableAsSelect() {
        // Produces the same plan as CREATE TABLE AS SELECT.
        verifyExplain(
                "REPLACE TABLE MyCtasTable\n"
                        + " WITH (\n"
                        + "   'connector' = 'values'\n"
                        + ") AS\n"
                        + "  SELECT\n"
                        + "    `a`,\n"
                        + "    `b`\n"
                        + "  FROM\n"
                        + "    MyTable",
                "testExplainCtas");
    }

    @Test
    void testExplainCreateTableAsSelectWithColumnsInCreateAndQueryParts() {
        verifyExplain(
                "CREATE TABLE MyCtasTable(\n"
                        + "  `votes` INT,\n"
                        + "  `votes_2x` AS `b` * 2,\n"
                        + "  `metadata_col` BIGINT METADATA,\n"
                        + "  `virtual_col` STRING METADATA VIRTUAL\n"
                        + ")\n"
                        + " WITH (\n"
                        + "   'connector' = 'values',\n"
                        + "   'readable-metadata' = 'metadata_col:BIGINT, virtual_col:STRING',\n"
                        + "   'writable-metadata' = 'metadata_col:BIGINT'\n"
                        + ") AS\n"
                        + "  SELECT\n"
                        + "    `a`,\n"
                        + "    `b`\n"
                        + "  FROM\n"
                        + "    MyTable",
                "testExplainCtasWithColumnsInCreateAndQueryParts");
    }

    @Test
    void testExplainReplaceTableAsSelectWithColumnsInCreateAndQueryParts() {
        // Produces the same plan as CREATE TABLE AS SELECT.
        verifyExplain(
                "REPLACE TABLE MyCtasTable(\n"
                        + "  `votes` INT,\n"
                        + "  `votes_2x` AS `b` * 2,\n"
                        + "  `metadata_col` BIGINT METADATA,\n"
                        + "  `virtual_col` STRING METADATA VIRTUAL\n"
                        + ")\n"
                        + " WITH (\n"
                        + "   'connector' = 'values',\n"
                        + "   'readable-metadata' = 'metadata_col:BIGINT, virtual_col:STRING',\n"
                        + "   'writable-metadata' = 'metadata_col:BIGINT'\n"
                        + ") AS\n"
                        + "  SELECT\n"
                        + "    `a`,\n"
                        + "    `b`\n"
                        + "  FROM\n"
                        + "    MyTable",
                "testExplainCtasWithColumnsInCreateAndQueryParts");
    }

    @Test
    void testExplainInsertWithChangelogModeMismatch() {
        createInsertOnlySink();
        assertThatThrownBy(
                        () ->
                                util.getTableEnv()
                                        .executeSql(
                                                "EXPLAIN CHANGELOG_MODE INSERT INTO MySink "
                                                        + JOIN_WITH_AGGREGATE_QUERY))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "Table sink 'default_catalog.default_database.MySink' doesn't support"
                                + " consuming update changes which is produced by node Join(")
                .hasMessageContaining(
                        "The update changes originate from:\n"
                                + "  - GroupAggregate(groupBy=[a], select=[a, SUM(b) AS total])"
                                + " (an aggregation updates its result when new rows arrive)")
                .hasMessageContaining(
                        "This query produces update changes, but the target table can't consume"
                                + " them. To fix it, write to a table that can consume update"
                                + " changes (for example, an upsert table with a PRIMARY KEY), or"
                                + " change the query so that it only produces inserts. To see"
                                + " which operators produce the update changes, run"
                                + " 'EXPLAIN CHANGELOG_MODE' on the SELECT part of the statement.");
    }

    @Test
    void testToDataStreamMismatchSuggestsToChangelogStream() {
        final StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
        tEnv.executeSql("CREATE TABLE src (a INT, b BIGINT) WITH ('connector' = 'values')");
        assertThatThrownBy(
                        () ->
                                tEnv.toDataStream(
                                        tEnv.sqlQuery("SELECT a, SUM(b) FROM src GROUP BY a")))
                .isInstanceOf(TableException.class)
                .hasMessageContaining("doesn't support consuming update changes")
                .hasMessageContaining("The update changes originate from:\n  - GroupAggregate(")
                .hasMessageContaining(
                        "toDataStream() only accepts inserts, but this query also produces update"
                                + " changes. To fix it, use tableEnv.toChangelogStream(table)")
                .hasMessageContaining("call table.explain(ExplainDetail.CHANGELOG_MODE).")
                .hasMessageNotContaining("PRIMARY KEY");
    }

    @Test
    void testInsertMismatchNamesUpsertSourceTableAsOrigin() {
        createInsertOnlySink();
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE MyUpsertTable (\n"
                                + "  a INT,\n"
                                + "  b BIGINT,\n"
                                + "  PRIMARY KEY (a) NOT ENFORCED\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'changelog-mode' = 'I,UA,D'\n"
                                + ")");
        assertThatThrownBy(
                        () ->
                                util.getTableEnv()
                                        .executeSql(
                                                "EXPLAIN CHANGELOG_MODE INSERT INTO MySink"
                                                        + " SELECT a, b, b FROM MyUpsertTable"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining("which is produced by node ChangelogNormalize(key=[a])")
                .hasMessageContaining(
                        "The update and delete changes originate from:\n"
                                + "  - source table"
                                + " 'default_catalog.default_database.MyUpsertTable'"
                                + " (an upsert or changelog table)");
    }

    @Test
    void testInsertMismatchCountsOriginsThatLookTheSame() {
        createInsertOnlySink();
        // three different aggregates whose plan nodes print the same text
        final String join =
                " JOIN (SELECT a, SUM(b) AS total FROM MyTable WHERE b > %1$d GROUP BY a) t%1$d"
                        + " ON o.a = t%1$d.a";
        assertThatThrownBy(
                        () ->
                                util.getTableEnv()
                                        .executeSql(
                                                "EXPLAIN CHANGELOG_MODE INSERT INTO MySink"
                                                        + " SELECT o.a, o.b,"
                                                        + " t1.total + t2.total + t3.total"
                                                        + " FROM MyTable o"
                                                        + String.format(join, 1)
                                                        + String.format(join, 2)
                                                        + String.format(join, 3)))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "The update changes originate from:\n"
                                + "  - GroupAggregate(groupBy=[a], select=[a, SUM(b) AS total])"
                                + " (an aggregation updates its result when new rows arrive)"
                                + " (3 operators)\n\n");
    }

    @Test
    void testInsertMismatchNamesOriginOfEachRejectedKind() {
        createInsertOnlySink();
        assertThatThrownBy(
                        () ->
                                util.getTableEnv()
                                        .executeSql(
                                                "EXPLAIN CHANGELOG_MODE INSERT INTO MySink"
                                                        + " SELECT o.a, o.b, t.total"
                                                        + " FROM MyTable o LEFT JOIN"
                                                        + " (SELECT a, SUM(b) AS total"
                                                        + " FROM MyTable GROUP BY a) t"
                                                        + " ON o.a = t.a"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "The update and delete changes originate from:\n"
                                + "  - GroupAggregate(groupBy=[a], select=[a, SUM(b) AS total])"
                                + " (an aggregation updates its result when new rows arrive)\n"
                                + "  - Join(joinType=[LeftOuterJoin]");
    }

    @Test
    void testInsertMismatchExplainsEarlyFiringWindow() {
        createInsertOnlySink();
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE MyProcTable (a INT, b BIGINT, ts AS PROCTIME())"
                                + " WITH ('connector' = 'values')");
        util.getTableEnv().getConfig().set("table.exec.emit.early-fire.enabled", "true");
        util.getTableEnv().getConfig().set("table.exec.emit.early-fire.delay", "1 s");
        assertThatThrownBy(
                        () ->
                                util.getTableEnv()
                                        .executeSql(
                                                "EXPLAIN CHANGELOG_MODE INSERT INTO MySink"
                                                        + " SELECT a, SUM(b), COUNT(*)"
                                                        + " FROM MyProcTable"
                                                        + " GROUP BY a, TUMBLE(ts, INTERVAL '1' MINUTE)"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "The update changes originate from:\n  - GroupWindowAggregate(")
                .hasMessageContaining(
                        "(a window with early or late firing emits a result before the window"
                                + " is final and updates it later)");
    }

    @Test
    void testInsertMismatchExplainsMultiJoin() {
        createInsertOnlySink();
        util.getTableEnv().getConfig().set("table.optimizer.multi-join.enabled", "true");
        assertThatThrownBy(
                        () ->
                                util.getTableEnv()
                                        .executeSql(
                                                "EXPLAIN CHANGELOG_MODE INSERT INTO MySink"
                                                        + " SELECT o.a, o.b, t.b FROM MyTable o"
                                                        + " LEFT JOIN MyTable t ON o.a = t.a"
                                                        + " LEFT JOIN MyTable u ON o.a = u.a"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "The update and delete changes originate from:\n  - MultiJoin(")
                .hasMessageContaining(
                        "(a regular join updates its result when an input changes, and an outer"
                                + " join also retracts rows padded with NULLs)");
    }

    @Test
    void testInsertMismatchExplainsProcessTableFunction() {
        createInsertOnlySink();
        util.getTableEnv().createTemporarySystemFunction("upsertPtf", UpdatingUpsertFunction.class);
        assertThatThrownBy(
                        () ->
                                util.getTableEnv()
                                        .executeSql(
                                                "EXPLAIN CHANGELOG_MODE INSERT INTO MySink"
                                                        + " SELECT a, `count`, `count`"
                                                        + " FROM upsertPtf(r => TABLE MyTable PARTITION BY a)"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "The update and delete changes originate from:\n"
                                + "  - ProcessTableFunction(invocation=[upsertPtf(")
                .hasMessageContaining(
                        "(the process table function declares that it emits updates or deletes)");
    }

    @Test
    void testStatementSetWithTwoMismatchedSinksReportsOneSinkAndItsCause() {
        createInsertOnlySink();
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE MySink2 (a INT, cnt BIGINT)"
                                + " WITH ('connector' = 'values', 'sink-insert-only' = 'true')");
        // planning stops at the first rejected sink, so the error is about exactly one of them
        assertThatThrownBy(
                        () ->
                                util.getTableEnv()
                                        .executeSql(
                                                "EXPLAIN CHANGELOG_MODE STATEMENT SET BEGIN"
                                                        + " INSERT INTO MySink "
                                                        + JOIN_WITH_AGGREGATE_QUERY
                                                        + "; INSERT INTO MySink2"
                                                        + " SELECT a, COUNT(*) FROM MyTable GROUP BY a;"
                                                        + " END"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "Table sink 'default_catalog.default_database.MySink' doesn't support"
                                + " consuming update changes which is produced by node Join(")
                .hasMessageContaining(
                        "The update changes originate from:\n"
                                + "  - GroupAggregate(groupBy=[a], select=[a, SUM(b) AS total])"
                                + " (an aggregation updates its result when new rows arrive)\n\n")
                .hasMessageNotContaining("MySink2")
                .hasMessageNotContaining("COUNT(*)");
    }

    @Test
    void testStatementSetWithSharedAggregateNamesOnlyTheRejectingSink() {
        util.getTableEnv()
                .executeSql(
                        "CREATE VIEW Totals AS SELECT a, SUM(b) AS total FROM MyTable GROUP BY a");
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE MyUpsertSink (a INT, total BIGINT,"
                                + " PRIMARY KEY (a) NOT ENFORCED)"
                                + " WITH ('connector' = 'values', 'sink-insert-only' = 'false')");
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE MyAppendSink (a INT, total BIGINT)"
                                + " WITH ('connector' = 'values', 'sink-insert-only' = 'true')");
        // the same GROUP BY feeds both sinks; only the append sink rejects its updates
        assertThatThrownBy(
                        () ->
                                util.getTableEnv()
                                        .executeSql(
                                                "EXPLAIN CHANGELOG_MODE STATEMENT SET BEGIN"
                                                        + " INSERT INTO MyUpsertSink SELECT a, total FROM Totals;"
                                                        + " INSERT INTO MyAppendSink SELECT a, total FROM Totals;"
                                                        + " END"))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "Table sink 'default_catalog.default_database.MyAppendSink' doesn't"
                                + " support consuming update changes")
                .hasMessageContaining(
                        "The update changes originate from:\n"
                                + "  - GroupAggregate(groupBy=[a], select=[a, SUM(b) AS total])"
                                + " (an aggregation updates its result when new rows arrive)\n\n")
                .hasMessageNotContaining("MyUpsertSink");
    }

    @Test
    void testExplainSelectShowsChangelogModeOfJoinWithAggregate() {
        final String explanation =
                util.getTableEnv()
                        .explainSql(JOIN_WITH_AGGREGATE_QUERY, ExplainDetail.CHANGELOG_MODE);
        assertThat(explanation)
                .contains("Join(joinType=[InnerJoin]")
                .contains(
                        "GroupAggregate(groupBy=[a], select=[a, SUM(b) AS total], changelogMode=[I,UA])");
    }

    private void createInsertOnlySink() {
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE MySink (\n"
                                + "  a INT,\n"
                                + "  b BIGINT,\n"
                                + "  total BIGINT\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'sink-insert-only' = 'true'\n"
                                + ")");
    }

    private void createMaterializedTableInCatalog(String createStatement) {
        TableEnvironmentInternal tableEnv = (TableEnvironmentInternal) util.getTableEnv();
        CreateMaterializedTableOperation operation =
                (CreateMaterializedTableOperation)
                        tableEnv.getParser().parse(createStatement).get(0);
        tableEnv.getCatalogManager()
                .createTable(
                        operation.getCatalogMaterializedTable(),
                        operation.getTableIdentifier(),
                        false);
    }

    private void verifyExplain(final String statement) {
        final String displayName = this.testInfo.getDisplayName();
        verifyExplain(statement, displayName.substring(0, displayName.length() - 2));
    }

    private void verifyExplain(final String statement, final String fileName) {
        final String actual = util.getTableEnv().explainSql(statement);
        final String fullFileName = fileName + ".out";
        if (REGENERATE_FILES) {
            writeToResource(fullFileName, actual);
            return;
        }
        final String expected = TableTestUtil.readFromResource("/explain/" + fullFileName);
        assertThat(TableTestUtil.replaceStageId(actual))
                .isEqualTo(TableTestUtil.replaceStageId(expected));
    }

    private void writeToResource(final String fileName, final String content) {
        try {
            final Path testClassesRoot = Paths.get(getClass().getResource("/").toURI());
            final Path resourcesRoot =
                    Paths.get(
                            testClassesRoot
                                    .toString()
                                    .replace("target/test-classes", "src/test/resources"));
            Files.writeString(resourcesRoot.resolve("explain").resolve(fileName), content);
        } catch (final Exception e) {
            throw new RuntimeException("Failed to regenerate golden file " + fileName, e);
        }
    }
}
