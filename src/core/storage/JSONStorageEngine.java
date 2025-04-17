package core.storage;

/**
 * ProjectName: gBase
 * ClassName: JSONStorageEngine
 * Package : core.parser
 * Description:
 *
 * @Author Lxl
 * @Create 2025/4/17 16:13
 * @Version 1.0
 */

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.core.type.TypeReference;
import core.metadata.*;
import java.io.*;
import java.util.*;
import java.util.stream.*;

/**
 * JSON格式的数据库存储引擎
 * 功能：
 * 1. 表数据与JSON格式的相互转换
 * 2. 支持完整CRUD操作
 * 3. 事务日志记录
 */
public class JSONStorageEngine {
    private static final ObjectMapper mapper = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private final FileSystemManager fsManager;

    public JSONStorageEngine(String databaseName) throws IOException {
        this.fsManager = new FileSystemManager(databaseName);
    }

    // === 表操作 ===
    public void createTable(Table table) throws IOException {
        fsManager.createTableFile(table.getName());
        saveTableMetadata(table);
    }

    public void dropTable(String tableName) throws IOException {
        fsManager.deleteTableFile(tableName);
    }

    // === 数据操作 ===
    public <T> List<T> selectAll(String tableName, Class<T> type) throws IOException {
        Path dataFile = fsManager.getTableDataFile(tableName);
        String json = fsManager.readFileContent(dataFile);
        return mapper.readValue(json, mapper.getTypeFactory()
                .constructCollectionType(List.class, type));
    }

    public <T> void insert(String tableName, T record) throws IOException {
        List<T> records = selectAll(tableName, (Class<T>) record.getClass());
        records.add(record);
        saveRecords(tableName, records);
    }

    public <T> void update(String tableName, Predicate<T> predicate, Consumer<T> updater)
            throws IOException {
        List<T> records = selectAll(tableName, (Class<T>) Object.class);
        records.stream()
                .filter(predicate)
                .forEach(updater);
        saveRecords(tableName, records);
    }

    public <T> void delete(String tableName, Predicate<T> predicate) throws IOException {
        List<T> records = selectAll(tableName, (Class<T>) Object.class);
        List<T> filtered = records.stream()
                .filter(predicate.negate())
                .collect(Collectors.toList());
        saveRecords(tableName, filtered);
    }

    // === 元数据操作 ===
    public void saveTableMetadata(Table table) throws IOException {
        String json = mapper.writeValueAsString(table);
        fsManager.writeFileContent(
                fsManager.getTableMetadataFile(table.getName()),
                json);
    }

    public Table loadTableMetadata(String tableName) throws IOException {
        String json = fsManager.readFileContent(
                fsManager.getTableMetadataFile(tableName));
        return mapper.readValue(json, Table.class);
    }

    // === 辅助方法 ===
    private <T> void saveRecords(String tableName, List<T> records) throws IOException {
        String json = mapper.writeValueAsString(records);
        fsManager.writeFileContent(
                fsManager.getTableDataFile(tableName),
                json);

        // 记录事务日志
        logTransaction(tableName, "UPDATE", records.size());
    }

    private void logTransaction(String tableName, String operation, int affectedRows)
            throws IOException {
        Path logFile = fsManager.getMetadataDir().resolve("transactions.log");
        String entry = String.format("[%s] %s %s rows=%d%n",
                new Date(), operation, tableName, affectedRows);

        Files.write(logFile, entry.getBytes(),
                StandardOpenOption.APPEND,
                StandardOpenOption.CREATE);
    }

    // === 批量操作 ===
    public <T> void batchInsert(String tableName, List<T> records) throws IOException {
        if (records.isEmpty()) return;

        List<T> existing = selectAll(tableName, (Class<T>) records.get(0).getClass());
        existing.addAll(records);
        saveRecords(tableName, existing);
    }

    public <T> List<T> query(String tableName, Predicate<T> predicate, Class<T> type)
            throws IOException {
        return selectAll(tableName, type).stream()
                .filter(predicate)
                .collect(Collectors.toList());
    }
}

