package core.metadata;

/**
 * ProjectName: gBase
 * ClassName: Database
 * Package : core.metadata
 * Description:
 *
 * @Author Lxl
 * @Create 2025/4/17 16:13
 * @Version 1.0
 */

import core.exception.DatabaseException;
import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * 数据库核心管理类
 * 功能：
 * 1. 连接池管理
 * 2. 事务控制
 * 3. SQL执行监控
 */
public class Database implements AutoCloseable {
    // 连接池配置
    private final BlockingQueue<Connection> connectionPool;
    private final int poolSize;
    private final String jdbcUrl;
    private final Properties connectionProps;

    // 事务状态
    private final ThreadLocal<Connection> currentTransaction = new ThreadLocal<>();
    private final ThreadLocal<Integer> transactionIsolationLevel = new ThreadLocal<>();

    /**
     * 构造函数
     * @param jdbcUrl JDBC连接字符串
     * @param username 数据库用户名
     * @param password 数据库密码
     * @param poolSize 连接池大小
     */
    public Database(String jdbcUrl, String username, String password, int poolSize) {
        this.jdbcUrl = jdbcUrl;
        this.poolSize = poolSize;
        this.connectionPool = new LinkedBlockingQueue<>(poolSize);

        this.connectionProps = new Properties();
        connectionProps.setProperty("user", username);
        connectionProps.setProperty("password", password);

        initializePool();
    }

    // 初始化连接池
    private void initializePool() {
        try {
            for (int i = 0; i < poolSize; i++) {
                connectionPool.add(createNewConnection());
            }
        } catch (SQLException e) {
            throw new DatabaseException("Connection pool initialization failed",
                    DatabaseException.CONNECTION_FAILURE,
                    null,
                    Map.of("poolSize", poolSize),
                    e);
        }
    }

    // 创建新连接
    private Connection createNewConnection() throws SQLException {
        Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps);
        // 优化连接配置
        conn.setAutoCommit(true);
        conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        return conn;
    }

    /**
     * 获取数据库连接（自动管理连接池）
     * @return 可用数据库连接
     */
    public Connection getConnection() {
        // 如果当前有事务，返回事务连接
        if (currentTransaction.get() != null) {
            return currentTransaction.get();
        }

        try {
            Connection conn = connectionPool.poll(5, TimeUnit.SECONDS);
            if (conn == null) {
                throw new DatabaseException("Connection timeout",
                        DatabaseException.CONNECTION_TIMEOUT,null,null);
            }
            return conn;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DatabaseException("Connection interrupted",
                    DatabaseException.CONNECTION_INTERRUPTED,
                    null,
                    null,
                    e);
        }
    }

    /**
     * 释放连接回连接池
     * @param conn 要释放的连接
     */
    public void releaseConnection(Connection conn) {
        if (conn == currentTransaction.get()) {
            return; // 事务连接不释放
        }

        try {
            if (!conn.isClosed() && conn.isValid(1)) {
                if (!connectionPool.offer(conn)) {
                    conn.close(); // 如果连接池已满则直接关闭
                }
            }
        } catch (SQLException e) {
            try { conn.close(); } catch (SQLException ignored) {}
        }
    }

    // === 事务控制 ===
    /**
     * 开始事务
     * @param isolationLevel 事务隔离级别
     */
    public void beginTransaction(int isolationLevel) {
        if (currentTransaction.get() != null) {
            throw new DatabaseException("Nested transaction not supported",
                    DatabaseException.TRANSACTION_ERROR,null,null);
        }

        try {
            Connection conn = getConnection();
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(isolationLevel);
            currentTransaction.set(conn);
            transactionIsolationLevel.set(isolationLevel);
        } catch (SQLException e) {
            throw new DatabaseException("Transaction start failed",
                    DatabaseException.TRANSACTION_ERROR,
                    null,
                    Map.of("isolationLevel", isolationLevel),
                    e);
        }
    }

    /**
     * 提交当前事务
     */
    public void commit() {
        Connection conn = currentTransaction.get();
        if (conn == null) {
            throw new DatabaseException("No active transaction to commit",
                    DatabaseException.TRANSACTION_ERROR,null,null);
        }

        try {
            conn.commit();
        } catch (SQLException e) {
            throw new DatabaseException("Transaction commit failed",
                    DatabaseException.TRANSACTION_ERROR,
                    null,
                    null,
                    e);
        } finally {
            cleanupTransaction();
        }
    }

    /**
     * 回滚当前事务
     */
    public void rollback() {
        Connection conn = currentTransaction.get();
        if (conn == null) {
            throw new DatabaseException("No active transaction to rollback",
                    DatabaseException.TRANSACTION_ERROR,null,null);
        }

        try {
            conn.rollback();
        } catch (SQLException e) {
            throw new DatabaseException("Transaction rollback failed",
                    DatabaseException.TRANSACTION_ERROR,
                    null,
                    null,
                    e);
        } finally {
            cleanupTransaction();
        }
    }

    // 清理事务状态
    private void cleanupTransaction() {
        Connection conn = currentTransaction.get();
        try {
            if (conn != null) {
                conn.setAutoCommit(true);
                releaseConnection(conn);
            }
        } catch (SQLException ignored) {
        } finally {
            currentTransaction.remove();
            transactionIsolationLevel.remove();
        }
    }

    // === SQL执行方法 ===
    /**
     * 执行查询SQL
     * @param sql SQL语句
     * @param params 参数列表
     * @return ResultSet
     */
    public ResultSet executeQuery(String sql, Object... params) {
        Connection conn = getConnection();
        try {
            PreparedStatement stmt = conn.prepareStatement(sql);
            bindParameters(stmt, params);
            return stmt.executeQuery();
        } catch (SQLException e) {
            releaseConnection(conn);
            throw convertSqlException(e, sql, params);
        }
    }

    /**
     * 执行更新SQL
     * @param sql SQL语句
     * @param params 参数列表
     * @return 影响的行数
     */
    public int executeUpdate(String sql, Object... params) {
        Connection conn = getConnection();
        try {
            PreparedStatement stmt = conn.prepareStatement(sql);
            bindParameters(stmt, params);
            return stmt.executeUpdate();
        } catch (SQLException e) {
            releaseConnection(conn);
            throw convertSqlException(e, sql, params);
        } finally {
            if (conn != currentTransaction.get()) {
                releaseConnection(conn);
            }
        }
    }

    // 参数绑定
    private void bindParameters(PreparedStatement stmt, Object[] params) throws SQLException {
        if (params != null) {
            for (int i = 0; i < params.length; i++) {
                stmt.setObject(i + 1, params[i]);
            }
        }
    }

    // SQL异常转换
    private DatabaseException convertSqlException(SQLException e, String sql, Object[] params) {
        Map<String, Object> context = new HashMap<>();
        context.put("sql", sql);
        if (params != null) {
            context.put("parameters", Arrays.toString(params));
        }

        int errorCode;
        if (e.getSQLState().startsWith("42")) { // SQL语法错误
            errorCode = DatabaseException.SQL_SYNTAX_ERROR;
        } else if (e.getSQLState().startsWith("23")) { // 约束违反
            errorCode = DatabaseException.CONSTRAINT_VIOLATION;
        } else {
            errorCode = DatabaseException.UNKNOWN_ERROR;
        }

        return new DatabaseException(e.getMessage(),
                errorCode,
                sql,
                context,
                e);
    }

    // === 连接池管理 ===
    @Override
    public void close() {
        connectionPool.forEach(conn -> {
            try {
                if (!conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException ignored) {}
        });
        connectionPool.clear();
    }

    /**
     * 检查连接池状态
     * @return 当前可用连接数
     */
    public int getAvailableConnections() {
        return connectionPool.size();
    }

    /**
     * 测试连接有效性
     * @return 是否所有连接有效
     */
    public boolean validatePool() {
        return connectionPool.stream().allMatch(conn -> {
            try {
                return conn.isValid(1);
            } catch (SQLException e) {
                return false;
            }
        });
    }
}
