package core.metadata;

/**
 * ProjectName: gBase
 * ClassName: Column
 * Package : core.metadata
 * Description:
 *
 * @Author Lxl
 * @Create 2025/4/17 16:13
 * @Version 1.0
 */

import core.exception.DatabaseException;

import java.math.BigDecimal;
import java.sql.JDBCType;
import java.util.*;

/**
 * 数据库列定义
 * 功能：
 * 1. 列元数据存储
 * 2. 类型转换
 * 3. DDL语句生成
 */
public class Column {
    public enum Constraint {
        PRIMARY_KEY,
        NOT_NULL,
        UNIQUE,
        AUTO_INCREMENT,
        DEFAULT
    }

    private final String name;
    private final JDBCType jdbcType;
    private final Integer length;
    private final Integer precision;
    private final Integer scale;
    private final Set<Constraint> constraints;
    private final String defaultValue;

    private Column(Builder builder) {
        this.name = Objects.requireNonNull(builder.name, "Column name cannot be null");
        this.jdbcType = Objects.requireNonNull(builder.jdbcType, "JDBC type cannot be null");
        this.length = builder.length;
        this.precision = builder.precision;
        this.scale = builder.scale;
        this.constraints = Collections.unmodifiableSet(new HashSet<>(builder.constraints));
        this.defaultValue = builder.defaultValue;
    }

    // === 构建器模式 ===
    public static Builder builder(String name, JDBCType type) {
        return new Builder(name, type);
    }

    public static class Builder {
        private final String name;
        private final JDBCType jdbcType;
        private Integer length;
        private Integer precision;
        private Integer scale;
        private final Set<Constraint> constraints = new HashSet<>();
        private String defaultValue;

        public Builder(String name, JDBCType jdbcType) {
            this.name = name;
            this.jdbcType = jdbcType;
        }

        public Builder length(int length) {
            this.length = length;
            return this;
        }

        public Builder precision(int precision) {
            this.precision = precision;
            return this;
        }

        public Builder scale(int scale) {
            this.scale = scale;
            return this;
        }

        public Builder addConstraint(Constraint constraint) {
            this.constraints.add(constraint);
            return this;
        }

        public Builder defaultValue(String value) {
            this.defaultValue = value;
            return this;
        }

        public Column build() {
            return new Column(this);
        }
    }

    // === 核心方法 ===
    /**
     * 生成列定义DDL片段
     */
    public String toDDL() {
        StringBuilder sb = new StringBuilder(name).append(" ").append(jdbcType.name());

        // 处理长度/精度
        if (length != null && length > 0) {
            sb.append("(").append(length);
            if (scale != null && scale > 0) {
                sb.append(",").append(scale);
            }
            sb.append(")");
        } else if (precision != null && precision > 0) {
            sb.append("(").append(precision);
            if (scale != null && scale > 0) {
                sb.append(",").append(scale);
            }
            sb.append(")");
        }

        // 处理约束
        for (Constraint c : constraints) {
            switch (c) {
                case NOT_NULL:
                    sb.append(" NOT NULL");
                    break;
                case UNIQUE:
                    sb.append(" UNIQUE");
                    break;
                case AUTO_INCREMENT:
                    sb.append(" AUTO_INCREMENT");
                    break;
                case DEFAULT:
                    if (defaultValue != null) {
                        sb.append(" DEFAULT ").append(formatDefaultValue());
                    }
                    break;
            }
        }

        return sb.toString();
    }

    private String formatDefaultValue() {
        switch (jdbcType) {
            case VARCHAR:
            case CHAR:
            case DATE:
            case TIMESTAMP:
                return "'" + defaultValue + "'";
            default:
                return defaultValue;
        }
    }

    // === 类型转换 ===
    public Object convertToJdbcType(Object value) {
        try {
            switch (jdbcType) {
                case INTEGER:
                    return Integer.valueOf(value.toString());
                case BIGINT:
                    return Long.valueOf(value.toString());
                case DECIMAL:
                    return new BigDecimal(value.toString());
                case BOOLEAN:
                    return Boolean.valueOf(value.toString());
                case TIMESTAMP:
                    return java.sql.Timestamp.valueOf(value.toString());
                default:
                    return value;
            }
        } catch (Exception e) {
            throw new DatabaseException("Type conversion failed",
                    DatabaseException.TYPE_CONVERSION_ERROR,
                    null,
                    Map.of(
                            "column", name,
                            "value", value,
                            "targetType", jdbcType
                    ),
                    e);
        }
    }

    // === 访问方法 ===
    public String getName() { return name; }
    public JDBCType getJdbcType() { return jdbcType; }
    public JDBCType getType() { return jdbcType; } // Alias for compatibility with test code
    public boolean isPrimaryKey() { return constraints.contains(Constraint.PRIMARY_KEY); }
    public boolean isNullable() { return !constraints.contains(Constraint.NOT_NULL); }
    public String getDefaultValue() { return defaultValue; }
    public Integer getLength() { return length; }
    public Integer getPrecision() { return precision; }
    public Integer getScale() { return scale; }

    /**
     * 添加约束
     * @param constraint 要添加的约束
     */
    public void addConstraint(Constraint constraint) {
        ((HashSet<Constraint>)constraints).add(constraint);
    }
}
