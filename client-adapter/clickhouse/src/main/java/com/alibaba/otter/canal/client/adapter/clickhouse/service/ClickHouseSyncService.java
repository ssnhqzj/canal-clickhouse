package com.alibaba.otter.canal.client.adapter.clickhouse.service;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

import com.alibaba.otter.canal.client.adapter.clickhouse.support.BatchExecutor;
import com.alibaba.otter.canal.client.adapter.clickhouse.support.SingleDml;
import com.alibaba.otter.canal.client.adapter.clickhouse.support.SyncUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter.Feature;
import com.alibaba.otter.canal.client.adapter.clickhouse.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.clickhouse.config.MappingConfig.DbMapping;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.Util;
import org.springframework.util.CollectionUtils;

/**
 * RDB同步操作业务
 *
 * @author rewerma 2018-11-7 下午06:45:49
 * @version 1.0.0
 */
public class ClickHouseSyncService {

    private static final Logger logger = LoggerFactory.getLogger(ClickHouseSyncService.class);

    private DruidDataSource dataSource;
    // 源库表字段类型缓存: instance.schema.table -> <columnName, jdbcType>
    private Map<String, Map<String, Integer>> columnsTypeCache;

    private int threads = 3;
    private boolean skipDupException;

    private List<SyncItem>[] dmlsPartition;
    private BatchExecutor[] batchExecutors;
    private ExecutorService[] executorThreads;

    public List<SyncItem>[] getDmlsPartition() {
        return dmlsPartition;
    }

    public Map<String, Map<String, Integer>> getColumnsTypeCache() {
        return columnsTypeCache;
    }

    public ClickHouseSyncService(DruidDataSource dataSource, Integer threads, boolean skipDupException) {
        this(dataSource, threads, new ConcurrentHashMap<>(), skipDupException);
    }

    @SuppressWarnings("unchecked")
    public ClickHouseSyncService(DruidDataSource dataSource, Integer threads, Map<String, Map<String, Integer>> columnsTypeCache,
                                 boolean skipDupException) {
        this.dataSource = dataSource;
        this.columnsTypeCache = columnsTypeCache;
        this.skipDupException = skipDupException;
        try {
            if (threads != null) {
                this.threads = threads;
            }
            this.dmlsPartition = new List[this.threads];
            this.batchExecutors = new BatchExecutor[this.threads];
            this.executorThreads = new ExecutorService[this.threads];
            for (int i = 0; i < this.threads; i++) {
                dmlsPartition[i] = new ArrayList<>();
                batchExecutors[i] = new BatchExecutor(dataSource);
                executorThreads[i] = Executors.newSingleThreadExecutor();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 批量同步回调
     *
     * @param dmls     批量 DML
     * @param function 回调方法
     */
    public void sync(List<Dml> dmls, Function<Dml, Boolean> function) {
        try {
            boolean toExecute = false;
            for (Dml dml : dmls) {
                if (!toExecute) {
                    toExecute = function.apply(dml);
                } else {
                    function.apply(dml);
                }
            }
            if (toExecute) {
                List<Future<Boolean>> futures = new ArrayList<>();
                for (int i = 0; i < threads; i++) {
                    int j = i;
                    if (dmlsPartition[j].isEmpty()) {
                        // bypass
                        continue;
                    }

                    futures.add(executorThreads[i].submit(() -> {
                        try {
                            dmlsPartition[j].forEach(syncItem -> sync(batchExecutors[j],
                                    syncItem.config,
                                    syncItem.singleDml));
                            dmlsPartition[j].clear();
                            batchExecutors[j].commit();
                            return true;
                        } catch (Throwable e) {
                            dmlsPartition[j].clear();
                            batchExecutors[j].rollback();
                            throw new RuntimeException(e);
                        }
                    }));
                }

                futures.forEach(future -> {
                    try {
                        future.get();
                    } catch (ExecutionException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        } finally {
            for (BatchExecutor batchExecutor : batchExecutors) {
                if (batchExecutor != null) {
                    batchExecutor.close();
                }
            }
        }
    }

    /**
     * 批量同步
     *
     * @param mappingConfig 配置集合
     * @param dmls          批量 DML
     */
    public void sync(Map<String, Map<String, MappingConfig>> mappingConfig, List<Dml> dmls, Properties envProperties) {
        sync(dmls, dml -> {
            if (dml.getIsDdl() != null && dml.getIsDdl() && StringUtils.isNotEmpty(dml.getSql())) {
                // DDL
                columnsTypeCache.remove(dml.getDestination() + "." + dml.getDatabase() + "." + dml.getTable());
                return false;
            } else {
                // DML
                String destination = StringUtils.trimToEmpty(dml.getDestination());
                String groupId = StringUtils.trimToEmpty(dml.getGroupId());
                String database = dml.getDatabase();
                String table = dml.getTable();
                Map<String, MappingConfig> configMap;
                if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
                    configMap = mappingConfig.get(destination + "-" + groupId + "_" + database + "-" + table);
                } else {
                    configMap = mappingConfig.get(destination + "_" + database + "-" + table);
                }

                if (configMap == null) {
                    return false;
                }

                if (configMap.values().isEmpty()) {
                    return false;
                }

                for (MappingConfig config : configMap.values()) {
                    appendDmlPartition(config, dml);
                }
                return true;
            }
        });
    }

    /**
     * 将Dml加入 {@link #dmlsPartition}
     *
     * @param config 表映射配置
     * @param dml    Dml对象
     */
    public void appendDmlPartition(MappingConfig config, Dml dml) {
        boolean caseInsensitive = config.getDbMapping().isCaseInsensitive();
        if (config.getConcurrent()) {
            List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml, caseInsensitive);
            singleDmls.forEach(singleDml -> {
                int hash = pkHash(config.getDbMapping(), singleDml.getData());
                SyncItem syncItem = new SyncItem(config, singleDml);
                dmlsPartition[hash].add(syncItem);
            });
        } else {
            int hash = 0;
            List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml, caseInsensitive);
            singleDmls.forEach(singleDml -> {
                SyncItem syncItem = new SyncItem(config, singleDml);
                dmlsPartition[hash].add(syncItem);
            });
        }
    }

    /**
     * 单条 dml 同步
     *
     * @param batchExecutor 批量事务执行器
     * @param config        对应配置对象
     * @param dml           DML
     */
    public void sync(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) {
        if (config != null) {
            try {
                String type = dml.getType();
                if (type != null && type.equalsIgnoreCase("INSERT")) {
                    insert(batchExecutor, config, dml);
                } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
                    update(batchExecutor, config, dml);
                } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                    delete(batchExecutor, config, dml);
                } else if (type != null && type.equalsIgnoreCase("TRUNCATE")) {
                    truncate(batchExecutor, config);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("DML: {}", JSON.toJSONString(dml, Feature.WriteNulls));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 插入操作
     *
     * @param config 配置项
     * @param dml    DML数据
     */
    private void insert(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) throws SQLException {
        Map<String, Object> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }
        String insertSql = buildInsertSql(config, data);
        if (StringUtils.isBlank(insertSql)) {
            return;
        }
        List<Map<String, ?>> values = getTargetColumnValues(batchExecutor, config, data);
        try {
            batchExecutor.execute(insertSql, values);
        } catch (SQLException e) {
            if (skipDupException
                    && (e.getMessage().contains("Duplicate entry") || e.getMessage().startsWith("ORA-00001:"))) {
            } else {
                throw e;
            }
        }
        if (logger.isTraceEnabled()) {
            logger.trace("Insert into target table, sql: {}", insertSql);
        }
    }


    /**
     * 更新操作
     *
     * @param config 配置项
     * @param dml    DML数据
     */
    private void update(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) throws SQLException {
        Map<String, Object> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }
        Map<String, Object> oldData = dml.getOld();
        if (CollectionUtils.isEmpty(oldData)) {
            oldData = new HashMap<>();
        }
        String deleteSql = buildDeleteSql(config, data, oldData);
        if (StringUtils.isBlank(deleteSql)) {
            return;
        }
        List<Map<String, ?>> deleteValues = getTargetColumnValues(batchExecutor, config, oldData);

        String insertSql = buildInsertSql(config, data);
        if (StringUtils.isBlank(insertSql)) {
            return;
        }
        List<Map<String, ?>> insertValues = getTargetColumnValues(batchExecutor, config, data);
        try {
            batchExecutor.execute(deleteSql, deleteValues);
            batchExecutor.execute(insertSql, insertValues);
        } catch (SQLException e) {
            if (skipDupException
                    && (e.getMessage().contains("Duplicate entry") || e.getMessage().startsWith("ORA-00001:"))) {
                // ignore
                // 增加更多关系数据库的主键冲突的错误码
            } else {
                throw e;
            }
        }
    }

    /**
     * 删除操作
     */
    private void delete(BatchExecutor batchExecutor, MappingConfig config, SingleDml dml) throws SQLException {
        Map<String, Object> data = dml.getData();
        if (data == null || data.isEmpty()) {
            return;
        }
        Map<String, Object> oldData = dml.getOld();
        if (CollectionUtils.isEmpty(oldData)) {
            oldData = new HashMap<>();
        }
        String deleteSql = buildDeleteSql(config, data, oldData);
        List<Map<String, ?>> values = getTargetColumnValues(batchExecutor, config, oldData);
        try {
            batchExecutor.execute(deleteSql, values);
        } catch (SQLException e) {
            if (skipDupException
                    && (e.getMessage().contains("Duplicate entry") || e.getMessage().startsWith("ORA-00001:"))) {
                // ignore
                // 增加更多关系数据库的主键冲突的错误码
            } else {
                throw e;
            }
        }
        if (logger.isTraceEnabled()) {
            logger.trace("Delete from target table, sql: {}", deleteSql);
        }
    }

    /**
     * 构造插入sql语句
     */
    private String buildInsertSql(MappingConfig config, Map<String, Object> data) {
        if (CollectionUtils.isEmpty(data)) {
            return null;
        }
        DbMapping dbMapping = config.getDbMapping();
        String backtick = SyncUtil.getBacktickByDbType(dataSource.getDbType());
        Map<String, String> columnsMap = SyncUtil.getColumnsMap(dbMapping, data);
        if (StringUtils.isNotBlank(dbMapping.getTargetSign())) {
            Optional.ofNullable(columnsMap).orElse(new HashMap<>()).putIfAbsent(dbMapping.getTargetSign(), dbMapping.getTargetSign());
            data.put(dbMapping.getTargetSign(), 1);
        }
        if (CollectionUtils.isEmpty(columnsMap)) {
            return null;
        }

        StringBuilder insertSql = new StringBuilder();
        insertSql.append("INSERT INTO ").append(SyncUtil.getDbTableName(dbMapping, dataSource.getDbType())).append(" (");
        columnsMap.forEach((targetColumnName, srcColumnName) -> insertSql.append(backtick)
                .append(targetColumnName)
                .append(backtick)
                .append(","));
        int len = insertSql.length();
        insertSql.delete(len - 1, len).append(") VALUES (");
        int mapLen = columnsMap.size();
        for (int i = 0; i < mapLen; i++) {
            insertSql.append("?,");
        }
        len = insertSql.length();
        insertSql.delete(len - 1, len).append(")");
        return insertSql.toString();
    }

    /**
     * 构造删除sql语句
     */
    private String buildDeleteSql(MappingConfig config, Map<String, Object> data, Map<String, Object> oldData) {
        Optional.of(data).get().forEach(oldData::putIfAbsent);
        DbMapping dbMapping = config.getDbMapping();
        String backtick = SyncUtil.getBacktickByDbType(dataSource.getDbType());
        Map<String, String> columnsMap = SyncUtil.getColumnsMap(dbMapping, data);
        if (StringUtils.isNotBlank(dbMapping.getTargetSign())) {
            Optional.ofNullable(columnsMap).orElse(new HashMap<>()).putIfAbsent(dbMapping.getTargetSign(), dbMapping.getTargetSign());
            oldData.put(dbMapping.getTargetSign(), -1);
        }
        if (CollectionUtils.isEmpty(columnsMap)) {
            return null;
        }

        StringBuilder deleteSql = new StringBuilder();
        deleteSql.append("INSERT INTO ").append(SyncUtil.getDbTableName(dbMapping, dataSource.getDbType())).append(" (");

        columnsMap.forEach((targetColumnName, srcColumnName) -> deleteSql.append(backtick)
                .append(targetColumnName)
                .append(backtick)
                .append(","));
        int len = deleteSql.length();
        deleteSql.delete(len - 1, len).append(") VALUES (");
        int mapLen = columnsMap.size();
        for (int i = 0; i < mapLen; i++) {
            deleteSql.append("?,");
        }
        len = deleteSql.length();
        deleteSql.delete(len - 1, len).append(")");
        return deleteSql.toString();
    }

    /**
     * 获取目标列的数据
     */
    private List<Map<String, ?>> getTargetColumnValues(BatchExecutor batchExecutor, MappingConfig config, Map<String, Object> data) {
        Map<String, String> columnsMap = SyncUtil.getColumnsMap(config.getDbMapping(), data);
        Map<String, Integer> ctype = getTargetColumnType(batchExecutor.getConn(), config);
        List<Map<String, ?>> values = new ArrayList<>();
        for (Map.Entry<String, String> entry : columnsMap.entrySet()) {
            String targetColumnName = entry.getKey();
            String srcColumnName = entry.getValue();
            if (srcColumnName == null) {
                srcColumnName = Util.cleanColumn(targetColumnName);
            }

            Integer type = ctype.get(Util.cleanColumn(targetColumnName).toLowerCase());
            if (type == null) {
                throw new RuntimeException("Target column: " + targetColumnName + " not matched");
            }
            Object value = data.get(srcColumnName);
            BatchExecutor.setValue(values, type, value);
        }
        return values;
    }

    /**
     * 获取联合数据
     */
    private List<List<Map<String, ?>>> getUniteData(Connection conn, MappingConfig config) {
        String sql = config.getDbMapping().getSql();
        if (StringUtils.isBlank(sql)) {
            return null;
        }
        Map<String, String> columnsMap = SyncUtil.getColumnsMap(config.getDbMapping(), new HashMap<>());
        List<List<Map<String, ?>>> uniteDataList = new ArrayList<>();
        String backtick = SyncUtil.getBacktickByDbType(dataSource.getDbType());
        String conditionSql = SyncUtil.appendPkCondition(config.getDbMapping(), sql, backtick);
        Util.sqlRS(conn, conditionSql, rs -> {
            try {
                final Map<String, Integer> columnType = new HashMap<>();
                ResultSetMetaData rsd = rs.getMetaData();
                int columnCount = rsd.getColumnCount();
                for (int i = 1; i <= columnCount; i++) {
                    int colType = rsd.getColumnType(i);
                    // 修复year类型作为date处理时的data truncated问题
                    if ("YEAR".equals(rsd.getColumnTypeName(i))) {
                        colType = Types.VARCHAR;
                    }
                    columnType.put(rsd.getColumnName(i).toLowerCase(), colType);
                }

                while (rs.next()) {
                    List<Map<String, ?>> rowValues = new ArrayList<>();
                    for (Map.Entry<String, String> entry : columnsMap.entrySet()) {
                        String srcColumnName = entry.getValue();
                        Integer type = columnType.get(Util.cleanColumn(srcColumnName).toLowerCase());
                        if (type == null) {
                            throw new RuntimeException("Target column: " + srcColumnName + " not matched");
                        }
                        Object value = rs.getObject(srcColumnName);
                        BatchExecutor.setValue(rowValues, type, value);
                    }
                    uniteDataList.add(rowValues);
                }
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
            }
        });
        return uniteDataList;
    }

    /**
     * truncate操作
     */
    private void truncate(BatchExecutor batchExecutor, MappingConfig config) throws SQLException {
        DbMapping dbMapping = config.getDbMapping();
        StringBuilder sql = new StringBuilder();
        sql.append("TRUNCATE TABLE ").append(SyncUtil.getDbTableName(dbMapping, dataSource.getDbType()));
        batchExecutor.execute(sql.toString(), new ArrayList<>());
        if (logger.isTraceEnabled()) {
            logger.trace("Truncate target table, sql: {}", sql);
        }
    }

    /**
     * 获取目标字段类型
     *
     * @param conn   sql connection
     * @param config 映射配置
     * @return 字段sqlType
     */
    private Map<String, Integer> getTargetColumnType(Connection conn, MappingConfig config) {
        DbMapping dbMapping = config.getDbMapping();
        String cacheKey = config.getDestination() + "." + dbMapping.getDatabase() + "." + dbMapping.getTable();
        Map<String, Integer> columnType = columnsTypeCache.get(cacheKey);
        if (columnType == null) {
            synchronized (ClickHouseSyncService.class) {
                columnType = columnsTypeCache.get(cacheKey);
                if (columnType == null) {
                    columnType = new LinkedHashMap<>();
                    final Map<String, Integer> columnTypeTmp = columnType;
                    String sql = "SELECT * FROM " + SyncUtil.getDbTableName(dbMapping, dataSource.getDbType()) + " WHERE 1=2";
                    Util.sqlRS(conn, sql, rs -> {
                        try {
                            ResultSetMetaData rsd = rs.getMetaData();
                            int columnCount = rsd.getColumnCount();
                            for (int i = 1; i <= columnCount; i++) {
                                int colType = rsd.getColumnType(i);
                                // 修复year类型作为date处理时的data truncated问题
                                if ("YEAR".equals(rsd.getColumnTypeName(i))) {
                                    colType = Types.VARCHAR;
                                }
                                columnTypeTmp.put(rsd.getColumnName(i).toLowerCase(), colType);
                            }
                            columnsTypeCache.put(cacheKey, columnTypeTmp);
                        } catch (SQLException e) {
                            logger.error(e.getMessage(), e);
                        }
                    });
                }
            }
        }
        return columnType;
    }

    /**
     * 拼接主键 where条件
     */
    private void appendCondition(MappingConfig.DbMapping dbMapping, StringBuilder sql, Map<String, Integer> ctype,
                                 List<Map<String, ?>> values, Map<String, Object> d) {
        appendCondition(dbMapping, sql, ctype, values, d, null);
    }

    private void appendCondition(MappingConfig.DbMapping dbMapping, StringBuilder sql, Map<String, Integer> ctype,
                                 List<Map<String, ?>> values, Map<String, Object> d, Map<String, Object> o) {
        String backtick = SyncUtil.getBacktickByDbType(dataSource.getDbType());

        // 拼接主键
        for (Map.Entry<String, String> entry : dbMapping.getTargetPk().entrySet()) {
            String targetColumnName = entry.getKey();
            String srcColumnName = entry.getValue();
            if (srcColumnName == null) {
                srcColumnName = Util.cleanColumn(targetColumnName);
            }
            sql.append(backtick).append(targetColumnName).append(backtick).append("=? AND ");
            Integer type = ctype.get(Util.cleanColumn(targetColumnName).toLowerCase());
            if (type == null) {
                throw new RuntimeException("Target column: " + targetColumnName + " not matched");
            }
            // 如果有修改主键的情况
            if (o != null && o.containsKey(srcColumnName)) {
                BatchExecutor.setValue(values, type, o.get(srcColumnName));
            } else {
                BatchExecutor.setValue(values, type, d.get(srcColumnName));
            }
        }
        int len = sql.length();
        sql.delete(len - 4, len);
    }

    public static class SyncItem {

        private MappingConfig config;
        private SingleDml singleDml;

        public SyncItem(MappingConfig config, SingleDml singleDml) {
            this.config = config;
            this.singleDml = singleDml;
        }
    }

    /**
     * 取主键hash
     */
    public int pkHash(DbMapping dbMapping, Map<String, Object> d) {
        return pkHash(dbMapping, d, null);
    }

    public int pkHash(DbMapping dbMapping, Map<String, Object> d, Map<String, Object> o) {
        int hash = 0;
        // 取主键
        for (Map.Entry<String, String> entry : dbMapping.getTargetPk().entrySet()) {
            String targetColumnName = entry.getKey();
            String srcColumnName = entry.getValue();
            if (srcColumnName == null) {
                srcColumnName = Util.cleanColumn(targetColumnName);
            }
            Object value = null;
            if (o != null && o.containsKey(srcColumnName)) {
                value = o.get(srcColumnName);
            } else if (d != null) {
                value = d.get(srcColumnName);
            }
            if (value != null) {
                hash += value.hashCode();
            }
        }
        hash = Math.abs(hash) % threads;
        return Math.abs(hash);
    }

    public void close() {
        for (int i = 0; i < threads; i++) {
            executorThreads[i].shutdown();
        }
    }
}
