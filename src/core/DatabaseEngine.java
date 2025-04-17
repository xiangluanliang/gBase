package core;

/**
 * ProjectName: gBase
 * ClassName: DatabaseEngine
 * Package : core
 * Description:
 *
 * @Author Lxl
 * @Create 2025/4/17 16:10
 * @Version 1.0
 */

import core.exception.DatabaseException;
import core.exception.ParseException;
import core.exception.SyntaxException;
import core.metadata.Database;
import core.metadata.Table;
import core.parser.AlterCommand;
import core.parser.DDLParser;
import core.storage.JSONStorageEngine;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 数据库引擎主类
 * 职责：
 * 1. 管理所有数据库实例
 * 2. 协调DDL语句执行流程
 * 3. 处理与存储引擎的交互
 */
public class DatabaseEngine {
    private final JSONStorageEngine storageEngine;
    private final Map<String, Database> activeDatabases;
    private final DDLParser ddlParser;

    public DatabaseEngine() {
        this.storageEngine = new JSONStorageEngine();
        this.activeDatabases = new HashMap<>();
        this.ddlParser = new DDLParser();

        // 初始化时加载所有数据库（可选）
        loadAllDatabases();
    }

    /**
     * 创建新数据库
     * @param dbName 数据库名称
     * @throws DatabaseException 当数据库已存在时抛出
     */
    public void createDatabase(String dbName) {
        if (activeDatabases.containsKey(dbName)) {
            throw new DatabaseException("Database '" + dbName + "' already exists");
        }

        Database newDb = new Database(dbName);
        activeDatabases.put(dbName, newDb);

        try {
            storageEngine.saveDatabase(newDb);
            System.out.println("Database '" + dbName + "' created successfully");
        } catch (IOException e) {
            activeDatabases.remove(dbName); // 回滚内存状态
            throw new DatabaseException("Failed to save database: " + e.getMessage());
        }
    }

    /**
     * 删除数据库
     * @param dbName 数据库名称
     */
    public void dropDatabase(String dbName) {
        if (!activeDatabases.containsKey(dbName)) {
            throw new DatabaseException("Database '" + dbName + "' not found");
        }

        try {
            storageEngine.deleteDatabase(dbName);
            activeDatabases.remove(dbName);
            System.out.println("Database '" + dbName + "' dropped successfully");
        } catch (IOException e) {
            throw new DatabaseException("Failed to drop database: " + e.getMessage());
        }
    }

    /**
     * 执行DDL语句
     * @param dbName 目标数据库名
     * @param sql DDL语句
     */
    public void executeDDL(String dbName, String sql) {
        Database db = getDatabase(dbName);

        try {
            Object parseResult = ddlParser.parse(sql);

            if (parseResult instanceof Table) {
                // Handle CREATE TABLE
                Table table = (Table) parseResult;
                db.addTable(table);
                System.out.println("Table '" + table.getName() + "' created in database '" + db.getName() + "'");
            } else if (parseResult instanceof AlterCommand) {
                // Handle ALTER TABLE
                AlterCommand cmd = (AlterCommand) parseResult;
                cmd.execute(db);
                System.out.println("Table '" + cmd.getTableName() + "' altered successfully");
            } else if (parseResult instanceof String) {
                // Handle DROP TABLE
                String tableName = (String) parseResult;
                db.dropTable(tableName);
                System.out.println("Table '" + tableName + "' dropped from database '" + db.getName() + "'");
            } else {
                throw new SyntaxException("Unsupported DDL statement");
            }

            // 持久化变更
            storageEngine.saveDatabase(db);
        } catch (ParseException e) {
            throw new DatabaseException("SQL Parse Error: " + e.getMessage());
        } catch (IOException e) {
            throw new DatabaseException("Storage Error: " + e.getMessage());
        }
    }

    /**
     * 获取数据库对象
     * @param dbName 数据库名称
     * @return Database实例
     */
    public Database getDatabase(String dbName) {
        Database db = activeDatabases.get(dbName);
        if (db == null) {
            throw new DatabaseException("Database '" + dbName + "' not loaded");
        }
        return db;
    }

    // === 私有方法 ===

    private void loadAllDatabases() {
        try {
            for (String dbName : storageEngine.listDatabases()) {
                Database db = storageEngine.loadDatabase(dbName);
                activeDatabases.put(dbName, db);
            }
        } catch (IOException e) {
            System.err.println("Warning: Failed to load databases - " + e.getMessage());
        }
    }

    // The handling of DDL statements is now directly in the executeDDL method

    // === 状态检查方法 ===

    public boolean databaseExists(String dbName) {
        return activeDatabases.containsKey(dbName);
    }

    public void listDatabases() {
        if (activeDatabases.isEmpty()) {
            System.out.println("No databases exist");
            return;
        }

        System.out.println("Available databases:");
        activeDatabases.keySet().forEach(System.out::println);
    }

    public void showDatabaseSchema(String dbName) {
        Database db = getDatabase(dbName);
        System.out.println("Database: " + db.getName());
        db.getTables().forEach((name, table) -> {
            System.out.println("  Table: " + name);
            table.getColumns().forEach(col ->
                    System.out.println("    Column: " + col.getName() + " " + col.getType() +
                            (col.isNullable() ? " NULL" : " NOT NULL"))
            );
        });
    }
}

