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
    private final List<Column> columns;
    private final Set<String> primaryKeys;

    public Table(String name, String schema, List<Column> columns) {
        this.name = Objects.requireNonNull(name);
        this.schema = schema;
        this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
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
    public List<Column> getColumns() { return columns; }
    public Optional<Column> getColumn(String name) {
        return columns.stream()
                .filter(c -> c.getName().equalsIgnoreCase(name))
                .findFirst();
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
