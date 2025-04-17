package core.metadata;

/**
 * ProjectName: gBase
 * ClassName: Table
 * Package : core.metadata
 * Description:
 *
 * @Author Lxl
 * @Create 2025/4/17 16:13
 * @Version 1.0
 */

import core.metadata.Database;
import core.exception.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.ArrayList;

/**
 * 数据库表定义
 * 功能：
 * 1. 表元数据管理
 * 2. DDL操作
 * 3. 结构验证
 */
public class Table {
    private final String name;
    private final String schema;
    private List<Column> columns;
    private Set<String> primaryKeys;

    public Table(String name, String schema, List<Column> columns) {
        this.name = Objects.requireNonNull(name);
        this.schema = schema;
        this.columns = new ArrayList<>(columns);
        this.primaryKeys = columns.stream()
                .filter(Column::isPrimaryKey)
                .map(Column::getName)
                .collect(Collectors.toSet());

        validateStructure();
    }

    // === 结构验证 ===
    private void validateStructure() {
        if (columns.isEmpty()) {
            throw new DatabaseException("Table must have at least one column",
                    DatabaseException.INVALID_TABLE_DEFINITION,
                    null,
                    Map.of("table", name));
        }

        // 检查重复列名
        Set<String> columnNames = new HashSet<>();
        for (Column col : columns) {
            if (!columnNames.add(col.getName().toLowerCase())) {
                throw new DatabaseException("Duplicate column name: " + col.getName(),
                        DatabaseException.INVALID_TABLE_DEFINITION,
                        null,
                        Map.of("table", name, "column", col.getName()));
            }
        }
    }

    // === DDL操作 ===
    /**
     * 生成CREATE TABLE语句
     */
    public String generateCreateDDL() {
        StringBuilder sb = new StringBuilder("CREATE TABLE ");
        if (schema != null) {
            sb.append(schema).append(".");
        }
        sb.append(name).append(" (\n");

        // 列定义
        String columnDefs = columns.stream()
                .map(Column::toDDL)
                .collect(Collectors.joining(",\n  "));
        sb.append("  ").append(columnDefs);

        // 主键约束
        if (!primaryKeys.isEmpty()) {
            sb.append(",\n  CONSTRAINT pk_").append(name)
                    .append(" PRIMARY KEY (")
                    .append(String.join(", ", primaryKeys))
                    .append(")");
        }

        sb.append("\n)");
        return sb.toString();
    }

    /**
     * 在数据库中创建此表
     * @param db 数据库连接
     */
    public void createInDatabase(Database db) {
        String ddl = generateCreateDDL();
        try {
            db.executeUpdate(ddl);
        } catch (DatabaseException e) {
            throw new DatabaseException("Failed to create table",
                    DatabaseException.TABLE_CREATION_ERROR,
                    ddl,
                    Map.of("table", name),
                    e);
        }
    }

    // === 元数据查询 ===
    /**
     * 从数据库加载表结构
     * @param db 数据库连接
     * @param tableName 表名
     */
    public static Table loadFromDatabase(Database db, String tableName) {
        return loadFromDatabase(db, null, tableName);
    }

    public static Table loadFromDatabase(Database db, String schema, String tableName) {
        try (Connection conn = db.getConnection()) {
            DatabaseMetaData meta = conn.getMetaData();

            // 获取列信息
            List<Column> columns = new ArrayList<>();
            try (ResultSet rs = meta.getColumns(
                    null, schema, tableName, "%")) {

                while (rs.next()) {
                    Column col = Column.builder(
                                    rs.getString("COLUMN_NAME"),
                                    JDBCType.valueOf(rs.getInt("DATA_TYPE")))
                            .length(rs.getInt("COLUMN_SIZE"))
                            .precision(rs.getInt("DECIMAL_DIGITS"))
                            .scale(rs.getInt("NUM_PREC_RADIX"))
                            .build();

                    if ("NO".equals(rs.getString("IS_NULLABLE"))) {
                        col.addConstraint(Column.Constraint.NOT_NULL);
                    }
                    if (rs.getString("COLUMN_DEF") != null) {
                        col.defaultValue(rs.getString("COLUMN_DEF"));
                    }

                    columns.add(col);
                }
            }

            // 获取主键信息
            try (ResultSet rs = meta.getPrimaryKeys(
                    null, schema, tableName)) {

                while (rs.next()) {
                    String pkColumn = rs.getString("COLUMN_NAME");
                    columns.stream()
                            .filter(c -> c.getName().equals(pkColumn))
                            .findFirst()
                            .ifPresent(c -> c.addConstraint(Column.Constraint.PRIMARY_KEY));
                }
            }

            return new Table(tableName, schema, columns);
        } catch (SQLException e) {
            throw new DatabaseException("Failed to load table metadata",
                    DatabaseException.METADATA_ACCESS_ERROR,
                    null,
                    Map.of("table", tableName),
                    e);
        }
    }

    // === 访问方法 ===
    public String getName() { return name; }
    public String getSchema() { return schema; }
    public List<Column> getColumns() { return Collections.unmodifiableList(columns); }
    public Optional<Column> getColumn(String name) {
        return columns.stream()
                .filter(c -> c.getName().equalsIgnoreCase(name))
                .findFirst();
    }

    /**
     * 通过名称查找列
     * @param columnName 列名
     * @return 列对象，如果不存在则返回null
     */
    public Column getColumnByName(String columnName) {
        return getColumn(columnName).orElse(null);
    }

    /**
     * 添加列
     * @param column 要添加的列
     */
    public void addColumn(Column column) {
        // 检查列名是否重复
        if (getColumn(column.getName()).isPresent()) {
            throw new DatabaseException("Column '" + column.getName() + "' already exists",
                    DatabaseException.DUPLICATE_COLUMN,
                    null,
                    Map.of("table", name, "column", column.getName()));
        }

        // 添加列并更新主键集合
        List<Column> newColumns = new ArrayList<>(columns);
        newColumns.add(column);
        columns = newColumns;

        if (column.isPrimaryKey()) {
            primaryKeys.add(column.getName());
        }
    }

    /**
     * 删除列
     * @param columnName 要删除的列名
     */
    public void dropColumn(String columnName) {
        // 检查列是否存在
        Column column = getColumnByName(columnName);
        if (column == null) {
            throw new DatabaseException("Column '" + columnName + "' does not exist",
                    DatabaseException.COLUMN_NOT_FOUND,
                    null,
                    Map.of("table", name, "column", columnName));
        }

        // 检查是否是主键列
        if (column.isPrimaryKey()) {
            primaryKeys.remove(columnName);
        }

        // 移除列
        columns.removeIf(c -> c.getName().equalsIgnoreCase(columnName));
    }

    /**
     * 修改列定义
     * @param oldColumnName 原列名
     * @param newColumn 新的列定义
     */
    public void modifyColumn(String oldColumnName, Column newColumn) {
        // 检查原列是否存在
        Column oldColumn = getColumnByName(oldColumnName);
        if (oldColumn == null) {
            throw new DatabaseException("Column '" + oldColumnName + "' does not exist",
                    DatabaseException.COLUMN_NOT_FOUND,
                    null,
                    Map.of("table", name, "column", oldColumnName));
        }

        // 检查新列名是否与其他列冲突（如果名称改变）
        if (!oldColumnName.equalsIgnoreCase(newColumn.getName()) &&
                columns.stream()
                        .anyMatch(c -> !c.getName().equalsIgnoreCase(oldColumnName) &&
                                c.getName().equalsIgnoreCase(newColumn.getName()))) {
            throw new DatabaseException("Column name '" + newColumn.getName() + "' already exists",
                    DatabaseException.DUPLICATE_COLUMN,
                    null,
                    Map.of("table", name, "column", newColumn.getName()));
        }

        // 更新主键列表
        if (oldColumn.isPrimaryKey() && !newColumn.isPrimaryKey()) {
            primaryKeys.remove(oldColumnName);
        } else if (!oldColumn.isPrimaryKey() && newColumn.isPrimaryKey()) {
            primaryKeys.add(newColumn.getName());
        } else if (oldColumn.isPrimaryKey() && !oldColumnName.equals(newColumn.getName())) {
            primaryKeys.remove(oldColumnName);
            primaryKeys.add(newColumn.getName());
        }

        // 替换列定义
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equalsIgnoreCase(oldColumnName)) {
                columns.set(i, newColumn);
                break;
            }
        }
    }

    // === 实用方法 ===
    /**
     * 验证数据行是否符合表结构
     * @param row 数据行（列名->值）
     */
    public void validateRow(Map<String, Object> row) {
        for (Column col : columns) {
            if (col.isPrimaryKey() && row.get(col.getName()) == null) {
                throw new DatabaseException("Primary key cannot be null",
                        DatabaseException.CONSTRAINT_VIOLATION,
                        null,
                        Map.of("column", col.getName()));
            }

            try {
                if (row.containsKey(col.getName())) {
                    col.convertToJdbcType(row.get(col.getName()));
                }
            } catch (DatabaseException e) {
                throw new DatabaseException("Row validation failed",
                        DatabaseException.ROW_VALIDATION_ERROR,
                        null,
                        Map.of(
                                "table", name,
                                "column", col.getName(),
                                "value", row.get(col.getName())
                        ),
                        e);
            }
        }
    }
}
