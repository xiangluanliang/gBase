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
import core.exception.DatabaseException;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

/**
 * JSON格式的数据库存储引擎
 * 功能：
 * 1. 表数据与JSON格式的相互转换
 * 2. 支持完整CRUD操作
 * 3. DDL操作支持
 * 4. 事务日志记录
 */
public class JSONStorageEngine {
    private static final ObjectMapper mapper = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private final FileSystemManager fsManager;
    private final String databaseName;

    public JSONStorageEngine(String databaseName) throws IOException {
        this.databaseName = databaseName;
        this.fsManager = new FileSystemManager(databaseName);
    }

    // === 数据库操作 ===
    /**
     * 创建新数据库
     * @param dbName 数据库名称
     */
    public static void createDatabase(String dbName) throws IOException {
        Path dbPath = Paths.get("data", dbName);
        if (Files.exists(dbPath)) {
            throw new DatabaseException("Database already exists: " + dbName,
                    DatabaseException.DATABASE_ALREADY_EXISTS,
                    null,
                    Map.of("database", dbName));
        }
        
        // 创建数据库目录结构
        Files.createDirectories(dbPath);
        Files.createDirectories(dbPath.resolve("tables"));
        Files.createDirectories(dbPath.resolve("metadata"));
        
        // 初始化数据库元数据
        Path metadataFile = dbPath.resolve("metadata/database.json");
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("name", dbName);
        metadata.put("created", new Date().toString());
        metadata.put("version", "1.0");
        
        writeJson(metadataFile, metadata);
    }

    /**
     * 删除数据库
     * @param dbName 数据库名称
     */
    public static void dropDatabase(String dbName) throws IOException {
        Path dbPath = Paths.get("data", dbName);
        if (!Files.exists(dbPath)) {
            throw new DatabaseException("Database does not exist: " + dbName,
                    DatabaseException.DATABASE_NOT_FOUND,
                    null,
                    Map.of("database", dbName));
        }
        
        // 递归删除数据库目录
        Files.walk(dbPath)
                .sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        throw new DatabaseException("Failed to delete database file: " + path,
                                DatabaseException.DATABASE_DELETE_ERROR,
                                null,
                                Map.of("database", dbName, "path", path.toString()));
                    }
                });
    }

    // === 表操作 ===
    /**
     * 创建表
     * @param table 表定义对象
     */
    public void createTable(Table table) throws IOException {
        // 验证表名是否已存在
        if (fsManager.tableExists(table.getName())) {
            throw new DatabaseException("Table already exists: " + table.getName(),
                    DatabaseException.TABLE_ALREADY_EXISTS,
                    null,
                    Map.of("table", table.getName()));
        }

        // 创建表文件
        fsManager.createTableFile(table.getName());
        
        // 保存表元数据
        saveTableMetadata(table);
        
        // 记录创建操作
        logTransaction(table.getName(), "CREATE", 0);
    }

    /**
     * 删除表
     * @param tableName 表名
     */
    public void dropTable(String tableName) throws IOException {
        // 验证表是否存在
        if (!fsManager.tableExists(tableName)) {
            throw new DatabaseException("Table does not exist: " + tableName,
                    DatabaseException.TABLE_NOT_FOUND,
                    null,
                    Map.of("table", tableName));
        }

        // 删除表文件和元数据
        fsManager.deleteTableFile(tableName);
        
        // 记录删除操作
        logTransaction(tableName, "DROP", 0);
    }

    /**
     * 修改表结构 - 添加列
     * @param tableName 表名
     * @param newColumn 新列定义
     */
    public void addColumn(String tableName, Column newColumn) throws IOException {
        Table table = loadTableMetadata(tableName);
        table.addColumn(newColumn);
        
        // 更新所有现有记录，为新列添加默认值
        List<Map<String, Object>> records = selectAll(tableName, Map.class);
        records.forEach(record -> record.put(newColumn.getName(), newColumn.getDefaultValue()));
        
        // 保存更新后的元数据和数据
        saveTableMetadata(table);
        saveRecords(tableName, records);
        
        logTransaction(tableName, "ALTER_ADD_COLUMN", records.size());
    }

    /**
     * 修改表结构 - 删除列
     * @param tableName 表名
     * @param columnName 要删除的列名
     */
    public void dropColumn(String tableName, String columnName) throws IOException {
        Table table = loadTableMetadata(tableName);
        table.dropColumn(columnName);
        
        // 从所有记录中删除该列
        List<Map<String, Object>> records = selectAll(tableName, Map.class);
        records.forEach(record -> record.remove(columnName));
        
        // 保存更新后的元数据和数据
        saveTableMetadata(table);
        saveRecords(tableName, records);
        
        logTransaction(tableName, "ALTER_DROP_COLUMN", records.size());
    }

    /**
     * 修改表结构 - 修改列定义
     * @param tableName 表名
     * @param oldColumnName 原列名
     * @param newColumn 新列定义
     */
    public void modifyColumn(String tableName, String oldColumnName, Column newColumn) throws IOException {
        Table table = loadTableMetadata(tableName);
        table.modifyColumn(oldColumnName, newColumn);
        
        // 获取所有记录并转换数据类型
        List<Map<String, Object>> records = selectAll(tableName, Map.class);
        records.forEach(record -> {
            Object oldValue = record.remove(oldColumnName);
            if (oldValue != null) {
                try {
                    Object newValue = newColumn.convertToJdbcType(oldValue);
                    record.put(newColumn.getName(), newValue);
                } catch (Exception e) {
                    record.put(newColumn.getName(), newColumn.getDefaultValue());
                }
            } else {
                record.put(newColumn.getName(), newColumn.getDefaultValue());
            }
        });
        
        // 保存更新后的元数据和数据
        saveTableMetadata(table);
        saveRecords(tableName, records);
        
        logTransaction(tableName, "ALTER_MODIFY_COLUMN", records.size());
    }

    // === 辅助方法 ===
    private static void writeJson(Path path, Object data) throws IOException {
        String json = mapper.writeValueAsString(data);
        Files.write(path, json.getBytes(), StandardOpenOption.CREATE, 
                StandardOpenOption.TRUNCATE_EXISTING);
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

    private <T> void saveRecords(String tableName, List<T> records) throws IOException {
        String json = mapper.writeValueAsString(records);
        fsManager.writeFileContent(
                fsManager.getTableDataFile(tableName),
                json);
    }

    public <T> List<T> selectAll(String tableName, Class<T> type) throws IOException {
        Path dataFile = fsManager.getTableDataFile(tableName);
        String json = fsManager.readFileContent(dataFile);
        return mapper.readValue(json, mapper.getTypeFactory()
                .constructCollectionType(List.class, type));
    }

    /**
     * 获取数据库名称
     * @return 数据库名称
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * 检查数据库是否存在
     * @param dbName 数据库名称
     * @return 是否存在
     */
    public static boolean databaseExists(String dbName) {
        return Files.exists(Paths.get("data", dbName));
    }

    /**
     * 列出所有表
     * @return 表名列表
     */
    public List<String> listTables() throws IOException {
        return fsManager.listTables();
    }
}