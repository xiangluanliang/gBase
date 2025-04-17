package core.exception;

/**
 * ProjectName: gBase
 * ClassName: DatabaseException
 * Package : core.exception
 * Description:
 *
 * @Author Lxl
 * @Create 2025/4/17 16:11
 * @Version 1.0
 */

import java.util.Collections;
import java.util.Map;
import java.util.HashMap;

/**
 * 数据库异常基类
 * 职责：
 * 1. 封装所有数据库相关异常的公共特性
 * 2. 提供结构化错误信息
 * 3. 支持异常链和错误恢复
 */
public class DatabaseException extends RuntimeException {
    // 错误代码常量
    public static final int UNKNOWN_ERROR = 1000;
    public static final int SQL_SYNTAX_ERROR = 1001;
    public static final int TABLE_NOT_FOUND = 1002;
    public static final int CONSTRAINT_VIOLATION = 1003;
    public static final int TYPE_CONVERSION_ERROR = 1004;
    public static final int CONNECTION_FAILURE = 1005;
    public static final int CONNECTION_TIMEOUT = 1006;
    public static final int CONNECTION_INTERRUPTED = 1007;
    public static final int TRANSACTION_ERROR = 1008;
    public static final int INVALID_TABLE_DEFINITION = 1009;
    public static final int TABLE_CREATION_ERROR = 1010;
    public static final int METADATA_ACCESS_ERROR = 1011;
    public static final int ROW_VALIDATION_ERROR = 1012;

    private final int errorCode;
    private final String sqlStatement;
    private final Map<String, Object> context;

    /**
     * 基础构造函数
     * @param message 错误描述
     */
    public DatabaseException(String message) {
        this(message, UNKNOWN_ERROR, null, null);
    }

    /**
     * 完整构造函数
     * @param message 错误描述
     * @param errorCode 错误代码
     * @param sqlStatement 引发异常的SQL语句
     * @param context 错误上下文(key-value形式)
     */
    public DatabaseException(String message,
                             int errorCode,
                             String sqlStatement,
                             Map<String, Object> context) {
        super(message);
        this.errorCode = errorCode;
        this.sqlStatement = sqlStatement;
        this.context = context != null ? new HashMap<>(context) : null;
    }

    /**
     * 带原因的构造函数
     * @param message 错误描述
     * @param cause 原始异常
     */
    public DatabaseException(String message, Throwable cause) {
        this(message, UNKNOWN_ERROR, null, null, cause);
    }

    /**
     * 最完整构造函数
     * @param message 错误描述
     * @param errorCode 错误代码
     * @param sqlStatement SQL语句
     * @param context 上下文
     * @param cause 原始异常
     */
    public DatabaseException(String message,
                             int errorCode,
                             String sqlStatement,
                             Map<String, Object> context,
                             Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.sqlStatement = sqlStatement;
        this.context = context != null ? new HashMap<>(context) : null;
    }


    // === 构建器模式 ===
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String message;
        private int code = UNKNOWN_ERROR;
        private String sql;
        private Map<String, Object> context;
        private Throwable cause;

        public Builder message(String message) {
            this.message = message;
            return this;
        }

        public Builder code(int code) {
            this.code = code;
            return this;
        }

        public Builder sql(String sql) {
            this.sql = sql;
            return this;
        }

        public Builder context(String key, Object value) {
            if (this.context == null) {
                this.context = new HashMap<>();
            }
            this.context.put(key, value);
            return this;
        }

        public Builder cause(Throwable cause) {
            this.cause = cause;
            return this;
        }

        public DatabaseException build() {
            return new DatabaseException(message, code, sql, context, cause);
        }
    }

    // === 访问方法 ===
    public int getErrorCode() {
        return errorCode;
    }

    public String getSqlStatement() {
        return sqlStatement;
    }

    public Map<String, Object> getContext() {
        return context != null ? Collections.unmodifiableMap(context) : null;
    }

    /**
     * 获取格式化错误信息
     * @return 包含所有详细信息的字符串
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getName())
                .append(": ")
                .append(getMessage());

        if (errorCode != UNKNOWN_ERROR) {
            sb.append(" [Code: ").append(errorCode).append("]");
        }

        if (sqlStatement != null) {
            sb.append("\nSQL: ").append(sqlStatement);
        }

        if (context != null && !context.isEmpty()) {
            sb.append("\nContext:");
            context.forEach((k, v) -> sb.append("\n  ").append(k).append(" = ").append(v));
        }

        if (getCause() != null) {
            sb.append("\nCaused by: ").append(getCause().toString());
        }

        return sb.toString();
    }

    // === 常用工厂方法 ===
    public static DatabaseException tableNotFound(String tableName) {
        return new DatabaseException("Table not found: " + tableName,
                TABLE_NOT_FOUND,
                null,
                Collections.singletonMap("table", tableName));
    }

    public static DatabaseException constraintViolation(String constraintName) {
        return new DatabaseException("Constraint violation: " + constraintName,
                CONSTRAINT_VIOLATION,
                null,
                Collections.singletonMap("constraint", constraintName));
    }
}

