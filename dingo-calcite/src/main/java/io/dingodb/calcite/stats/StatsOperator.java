/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.calcite.stats;

import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.log.LogUtils;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.session.SessionUtil;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Utils;
import io.dingodb.meta.DdlService;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.entity.InfoSchema;
import io.dingodb.meta.entity.Table;
import io.dingodb.store.api.transaction.StoreKvTxn;
import io.dingodb.store.api.transaction.StoreTxnService;
import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import static io.dingodb.common.util.NameCaseUtils.convertName;
import static io.dingodb.common.util.NameCaseUtils.convertSql;

@Slf4j
/**
 * Provides shared helpers and static resources for stats metadata and analyze task operations.
 */
public abstract class StatsOperator {
    /** Global store transaction service for stats operations. */
    public static StoreTxnService storeTxnService;
    /** Meta service bound to the mysql schema. */
    public static MetaService metaService;

    /** Analyze task table name. */
    public static final String ANALYZE_TASK = "analyze_task";
    /** Table buckets metadata table name. */
    public static final String TABLE_BUCKETS = "table_buckets";
    /** Table statistics metadata table name. */
    public static final String TABLE_STATS = "table_stats";
    /** CM sketch metadata table name. */
    public static final String CM_SKETCH = "cm_sketch";

    /** Analyze task table metadata. */
    public static Table analyzeTaskTable;
    /** Table buckets metadata. */
    public static Table bucketsTable;
    /** Table stats metadata. */
    public static Table statsTable;
    /** CM sketch metadata. */
    public static Table cmSketchTable;
    /** Analyze task table id. */
    public static CommonId analyzeTaskTblId;
    /** Table buckets id. */
    public static CommonId bucketsTblId;
    /** Table stats id. */
    public static CommonId statsTblId;
    /** CM sketch table id. */
    public static CommonId cmSketchTblId;

    /** Analyze task key/value codec. */
    public static KeyValueCodec analyzeTaskCodec;
    /** Table buckets key/value codec. */
    public static KeyValueCodec bucketsCodec;
    /** Table stats key/value codec. */
    public static KeyValueCodec statsCodec;
    /** CM sketch key/value codec. */
    public static KeyValueCodec cmSketchCodec;

    /** Analyze task transaction store. */
    public static StoreKvTxn analyzeTaskStore;
    /** Table buckets transaction store. */
    public static StoreKvTxn bucketsStore;
    /** Table stats transaction store. */
    public static StoreKvTxn statsStore;
    /** CM sketch transaction store. */
    public static StoreKvTxn cmSketchStore;

    static {
        try {
            io.dingodb.meta.InfoSchemaService infoSchemaService = io.dingodb.meta.InfoSchemaService.root();
            while (!infoSchemaService.prepare()) {
                Utils.sleep(5000L);
            }
            storeTxnService = StoreTxnService.getDefault();
            metaService = MetaService.root().getSubMetaService(convertName("mysql"));
            analyzeTaskTable = getTable(ANALYZE_TASK);
            bucketsTable = getTable(TABLE_BUCKETS);
            statsTable = getTable(TABLE_STATS);
            cmSketchTable = getTable(CM_SKETCH);
            analyzeTaskTblId = analyzeTaskTable.tableId;
            bucketsTblId = bucketsTable.tableId;
            statsTblId = statsTable.tableId;
            cmSketchTblId = cmSketchTable.tableId;
            analyzeTaskCodec = CodecService.getDefault()
                .createKeyValueCodec(
                    analyzeTaskTable.getCodecVersion(), analyzeTaskTable.version,
                    analyzeTaskTable.tupleType(), analyzeTaskTable.keyMapping()
                );
            bucketsCodec = CodecService.getDefault()
                .createKeyValueCodec(bucketsTable.getCodecVersion(), bucketsTable.version,
                bucketsTable.tupleType(), bucketsTable.keyMapping());
            statsCodec = CodecService.getDefault()
                .createKeyValueCodec(statsTable.getCodecVersion(), statsTable.version,
                statsTable.tupleType(), statsTable.keyMapping());
            cmSketchCodec = CodecService.getDefault()
                .createKeyValueCodec(cmSketchTable.getCodecVersion(), cmSketchTable.version,
                cmSketchTable.tupleType(), cmSketchTable.keyMapping());
            analyzeTaskStore = storeTxnService.getInstance(analyzeTaskTblId,
                getRegionId(analyzeTaskTblId));
            bucketsStore = storeTxnService.getInstance(bucketsTblId, getRegionId(bucketsTblId));
            statsStore = storeTxnService.getInstance(statsTblId, getRegionId(statsTblId));
            cmSketchStore = storeTxnService
                .getInstance(cmSketchTblId, getRegionId(cmSketchTblId));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * Resolves the first region id for a table from its range distribution.
     *
     * @param tableId table id
     * @return region id
     */
    public static CommonId getRegionId(CommonId tableId) {
        return Optional.ofNullable(metaService.getRangeDistribution(tableId))
            .map(NavigableMap::firstEntry)
            .map(Map.Entry::getValue)
            .map(RangeDistribution::getId)
            .orElseThrow("Cannot get region for " + tableId);
    }

    /**
     * Inserts or updates each row in the provided list.
     *
     * @param store target kv transaction store
     * @param codec row codec
     * @param rowList rows to upsert
     */
    public static void upsert(StoreKvTxn store, KeyValueCodec codec, List<Object[]> rowList) {
        rowList.forEach(row -> {
            KeyValue old = store.get(codec.encodeKey(row));
            KeyValue keyValue = codec.encode(row);
            if (old == null || old.getValue() == null) {
                store.insert(keyValue.getKey(), keyValue.getValue());
            } else {
                store.update(keyValue.getKey(), keyValue.getValue());
            }
        });
    }

    /**
     * Deletes stats for the given schema and table (currently unused).
     *
     * @param schemaName schema name
     * @param tableName table name
     */
    public static void delStats(String schemaName, String tableName) {
    }

    /**
     * Deletes stats rows in the specified stats table.
     *
     * @param table stats table name
     * @param schemaName schema name
     * @param tableName table name
     */
    public static void delStats(String table, String schemaName, String tableName) {
        String sqlTemp = "delete from %s where schema_name='%s' and table_name='%s'";
        String sql = convertSql(String.format(sqlTemp, table, schemaName, tableName));
        String error = SessionUtil.INSTANCE.exeUpdateInTxn(sql);
        if (error != null) {
            LogUtils.error(log, "delStats error:{}, table:{}, schema:{}, tableName:{}",
                error, table, schemaName, tableName);
        }
    }

    /**
     * Scans a range distribution and decodes rows into object arrays.
     *
     * @param store kv transaction store
     * @param codec row codec
     * @param rangeDistribution scan range
     * @return decoded rows
     */
    public List<Object[]> scan(StoreKvTxn store, KeyValueCodec codec, RangeDistribution rangeDistribution) {
        try {
            Iterator<KeyValue> iterator = store.range(
                rangeDistribution.getStartKey(), rangeDistribution.getEndKey()
            );
            List<Object[]> list = new ArrayList<>();
            while (iterator.hasNext()) {
                list.add(codec.decode(iterator.next()));
            }
            return list;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Fetches a single row by key and decodes it.
     *
     * @param store kv transaction store
     * @param codec row codec
     * @param key row key values
     * @return decoded row or null when not found
     */
    public static Object[] get(StoreKvTxn store, KeyValueCodec codec, Object[] key) {
        try {
            KeyValue keyValue = store.get(codec.encodeKey(key));
            if (keyValue.getValue() == null || keyValue.getValue().length == 0) {
                return null;
            }
            return codec.decode(keyValue);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Builds analyze task primary key values for the given table.
     *
     * @param schemaName schema name
     * @param tableName table name
     * @return key values array
     */
    public Object[] getAnalyzeTaskKeys(String schemaName, String tableName) {
        Object[] values = new Object[analyzeTaskTable.getColumns().size()];
        values[0] = schemaName;
        values[1] = tableName;
        return values;
    }

    /**
     * Creates an analyze task row with initial metadata.
     *
     * @param schemaName schema name
     * @param tableName table name
     * @param totalCount table row count
     * @param modifyCount modified row count
     * @return row values for analyze task
     */
    public static Object[] generateAnalyzeTask(String schemaName,
                                               String tableName,
                                               long totalCount,
                                               long modifyCount) {
        return new Object[] {schemaName, tableName, "", totalCount, null, null,
            StatsTaskState.PENDING.getState(), null, DingoConfiguration.host(), modifyCount,
            new Timestamp(System.currentTimeMillis()), 0, 0, 0, 0};
    }

    /**
     * Looks up a table from the mysql schema, retrying for a limited time.
     *
     * @param tableName table name
     * @return resolved table
     */
    public static Table getTable(String tableName) {
        int times = 10;
        DdlService ddlService = DdlService.root();
        while (times-- > 0) {
            InfoSchema is = ddlService.getIsLatest();
            if (is != null) {
                Table table = is.getTable(convertName("mysql"), tableName);
                if (table != null) {
                    return table;
                }
            }
            try {
                Thread.sleep(10000L);
            } catch (Exception ignored) {

            }
        }
        throw new RuntimeException("init user error");
    }

}
