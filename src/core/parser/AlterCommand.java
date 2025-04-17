package core.parser;

/**
 * ProjectName: gBase
 * ClassName: AlterCommend
 * Package : core.parser
 * Description:
 *
 * @Author Lxl
 * @Create 2025/4/17 18:07
 * @Version 1.0
 */

import core.exception.DatabaseException;
import core.metadata.Column;
import core.metadata.Database;
import core.metadata.Table;

/**
 * AlterCommand represents an ALTER TABLE command
 * This class encapsulates the operations and parameters needed to modify a table structure
 */
public class AlterCommand {
    public enum AlterType {
        ADD_COLUMN,
        DROP_COLUMN,
        MODIFY_COLUMN
    }

    private final String tableName;
    private final AlterType alterType;
    private Column newColumn; // 用于 ADD 和 MODIFY 操作
    private String columnName; // 用于 DROP 操作或作为 MODIFY 操作中的旧列名

    /**
     * 用于 ADD_COLUMN 操作的构造函数
     * @param tableName 要修改的表的名称
     * @param alterType 必须是 ADD_COLUMN
     * @param newColumn 要添加的列
     */
    public AlterCommand(String tableName, AlterType alterType, Column newColumn) {
        if (alterType != AlterType.ADD_COLUMN) {
            throw new IllegalArgumentException("This constructor is only for ADD_COLUMN operations");
        }
        this.tableName = tableName;
        this.alterType = alterType;
        this.newColumn = newColumn;
    }

    /**
     * 用于 DROP_COLUMN 操作的构造函数
     * @param tableName 要修改的表的名称
     * @param alterType 必须是 DROP_COLUMN
     * @param columnName 要删除的列的名称
     */
    public AlterCommand(String tableName, AlterType alterType, String columnName) {
        if (alterType != AlterType.DROP_COLUMN) {
            throw new IllegalArgumentException("This constructor is only for DROP_COLUMN operations");
        }
        this.tableName = tableName;
        this.alterType = alterType;
        this.columnName = columnName;
    }

    /**
     * 用于 MODIFY_COLUMN 操作的构造函数
     * @param tableName 要修改的表的名称
     * @param alterType 必须是 MODIFY_COLUMN
     * @param columnName 要修改的列的名称
     * @param newColumn 新列的定义
     */
    public AlterCommand(String tableName, AlterType alterType, String columnName, Column newColumn) {
        if (alterType != AlterType.MODIFY_COLUMN) {
            throw new IllegalArgumentException("This constructor is only for MODIFY_COLUMN operations");
        }
        this.tableName = tableName;
        this.alterType = alterType;
        this.columnName = columnName;
        this.newColumn = newColumn;
    }

    /**
     * 在指定的数据库上执行 ALTER TABLE 命令
     * @param database 包含要修改的表的数据库
     */
    public void execute(Database database) {
        Table table = database.getTable(tableName);
        if (table == null) {
            throw new DatabaseException("Table '" + tableName + "' does not exist");
        }

        switch (alterType) {
            case ADD_COLUMN:
                executeAddColumn(table);
                break;
            case DROP_COLUMN:
                executeDropColumn(table);
                break;
            case MODIFY_COLUMN:
                executeModifyColumn(table);
                break;
        }
    }

    private void executeAddColumn(Table table) {
        // 检查列是否已存在
        if (table.getColumnByName(newColumn.getName()) != null) {
            throw new DatabaseException("Column '" + newColumn.getName() + "' already exists in table '" + tableName + "'");
        }
        table.addColumn(newColumn);
    }

    private void executeDropColumn(Table table) {
        // 检查列是否存在
        if (table.getColumnByName(columnName) == null) {
            throw new DatabaseException("Column '" + columnName + "' does not exist in table '" + tableName + "'");
        }
        table.dropColumn(columnName);
    }

    private void executeModifyColumn(Table table) {
        // 检查列是否存在
        if (table.getColumnByName(columnName) == null) {
            throw new DatabaseException("Column '" + columnName + "' does not exist in table '" + tableName + "'");
        }

        // 如果列被重命名（newColumn 中的名称与 columnName 不同）
        if (!columnName.equals(newColumn.getName())) {
            // 检查新名称是否已存在
            if (table.getColumnByName(newColumn.getName()) != null) {
                throw new DatabaseException("Cannot rename column '" + columnName +
                        "' to '" + newColumn.getName() + "' as the target name already exists");
            }
        }

        table.modifyColumn(columnName, newColumn);
    }

    /**
     * 获取被修改的表的名称
     * @return 表名称
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 获取修改类型
     * @return AlterType 枚举值
     */
    public AlterType getAlterType() {
        return alterType;
    }

    /**
     * 获取 DROP 操作中的列名称或 MODIFY 操作中的旧列名称
     * @return 列名称
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * 获取 ADD 或 MODIFY 操作中的新列定义
     * @return Column 对象
     */
    public Column getNewColumn() {
        return newColumn;
    }
}
