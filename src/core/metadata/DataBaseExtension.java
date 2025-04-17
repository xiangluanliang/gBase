package core.metadata;

/**
 * ProjectName: gBase
 * ClassName: DataBaseExtension
 * Package : core.metadata
 * Description:
 *
 * @Author Lxl
 * @Create 2025/4/17 18:08
 * @Version 1.0
 */

import core.exception.DatabaseException;
import core.parser.AlterCommand;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * DatabaseExtension
 *
 * Extension to the Database class that adds support for table operations
 * including adding, dropping, and modifying tables.
 */
public class Database {
    private final String name;
    private final Map<String, Table> tables;

    /**
     * 构造函数用于创建一个新的空数据库
     *
     * @param name 数据库名称
     */
    public Database(String name) {
        this.name = name;
        this.tables = new HashMap<>();
    }

    /**
     * 构造函数用于加载一个已存在的数据库
     *
     * @param name 数据库名称
     * @param tables 预加载的表
     */
    public Database(String name, Map<String, Table> tables) {
        this.name = name;
        this.tables = new HashMap<>(tables);
    }

    /**
     * 获取数据库名称
     *
     * @return 数据库名称
     */
    public String getName() {
        return name;
    }

    /**
     * 获取数据库中的所有表
     *
     * @return 包含表名称到表对象映射的Map
     */
    public Map<String, Table> getTables() {
        return Collections.unmodifiableMap(tables);
    }

    /**
     * 根据名称获取特定的表
     *
     * @param tableName 要检索的表的名称
     * @return 表对象或如果未找到则返回null
     */
    public Table getTable(String tableName) {
        return tables.get(tableName);
    }

    /**
     * 向数据库中添加一个表
     *
     * @param table 要添加的表对象
     * @throws DatabaseException 如果表已经存在
     */
    public void addTable(Table table) {
        String tableName = table.getName();
        if (tables.containsKey(tableName)) {
            throw new DatabaseException("表 '" + tableName + "' 已经存在于数据库 '" + name + "' 中",
                    DatabaseException.TABLE_ALREADY_EXISTS,
                    null,
                    Map.of("table", tableName, "database", name));
        }
        tables.put(tableName, table);
    }

    /**
     * 从数据库中删除一个表
     *
     * @param tableName 要删除的表的名称
     * @throws DatabaseException 如果表不存在
     */
    public void dropTable(String tableName) {
        if (!tables.containsKey(tableName)) {
            throw new DatabaseException("表 '" + tableName + "' 不存在于数据库 '" + name + "' 中",
                    DatabaseException.TABLE_NOT_FOUND,
                    null,
                    Map.of("table", tableName, "database", name));
        }
        tables.remove(tableName);
    }

    /**
     * 检查表是否存在于该数据库中
     *
     * @param tableName 要检查的表的名称
     * @return 如果表存在则返回true，否则返回false
     */
    public boolean tableExists(String tableName) {
        return tables.containsKey(tableName);
    }

    /**
     * 使用AlterCommand修改表
     *
     * @param command 包含修改详情的AlterCommand
     * @throws DatabaseException 如果表不存在或修改操作失败
     */
    public void alterTable(AlterCommand command) {
        String tableName = command.getTableName();
        Table table = getTable(tableName);

        if (table == null) {
            throw new DatabaseException("表 '" + tableName + "' 不存在于数据库 '" + name + "' 中",
                    DatabaseException.TABLE_NOT_FOUND,
                    null,
                    Map.of("table", tableName, "database", name));
        }

        // 直接在表上执行命令
        command.execute(this);
    }

    /**
     * 执行SQL更新语句
     *
     * @param sql 要执行的SQL语句
     * @param params SQL语句的参数
     * @return 受影响的行数
     * @throws DatabaseException 如果执行失败
     */
    public int executeUpdate(String sql, Object... params) {
        // 对于内存实现，这是一个存根
        // 在真实数据库中，这将执行SQL语句
        return 0;
    }
}
