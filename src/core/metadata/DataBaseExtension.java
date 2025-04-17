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
     * Constructor for creating a new empty database
     *
     * @param name Database name
     */
    public Database(String name) {
        this.name = name;
        this.tables = new HashMap<>();
    }

    /**
     * Constructor for loading an existing database
     *
     * @param name Database name
     * @param tables Pre-loaded tables
     */
    public Database(String name, Map<String, Table> tables) {
        this.name = name;
        this.tables = new HashMap<>(tables);
    }

    /**
     * Get the database name
     *
     * @return Database name
     */
    public String getName() {
        return name;
    }

    /**
     * Get all tables in this database
     *
     * @return Map of table name to Table objects
     */
    public Map<String, Table> getTables() {
        return Collections.unmodifiableMap(tables);
    }

    /**
     * Get a specific table by name
     *
     * @param tableName Name of the table to retrieve
     * @return Table object or null if not found
     */
    public Table getTable(String tableName) {
        return tables.get(tableName);
    }

    /**
     * Add a table to the database
     *
     * @param table Table object to add
     * @throws DatabaseException if table already exists
     */
    public void addTable(Table table) {
        String tableName = table.getName();
        if (tables.containsKey(tableName)) {
            throw new DatabaseException("Table '" + tableName + "' already exists in database '" + name + "'",
                    DatabaseException.TABLE_ALREADY_EXISTS,
                    null,
                    Map.of("table", tableName, "database", name));
        }
        tables.put(tableName, table);
    }

    /**
     * Drop a table from the database
     *
     * @param tableName Name of the table to drop
     * @throws DatabaseException if table does not exist
     */
    public void dropTable(String tableName) {
        if (!tables.containsKey(tableName)) {
            throw new DatabaseException("Table '" + tableName + "' does not exist in database '" + name + "'",
                    DatabaseException.TABLE_NOT_FOUND,
                    null,
                    Map.of("table", tableName, "database", name));
        }
        tables.remove(tableName);
    }

    /**
     * Check if a table exists in this database
     *
     * @param tableName Name of the table to check
     * @return true if the table exists, false otherwise
     */
    public boolean tableExists(String tableName) {
        return tables.containsKey(tableName);
    }

    /**
     * Alter a table using an AlterCommand
     *
     * @param command AlterCommand containing modification details
     * @throws DatabaseException if table does not exist or alter operation fails
     */
    public void alterTable(AlterCommand command) {
        String tableName = command.getTableName();
        Table table = getTable(tableName);

        if (table == null) {
            throw new DatabaseException("Table '" + tableName + "' does not exist in database '" + name + "'",
                    DatabaseException.TABLE_NOT_FOUND,
                    null,
                    Map.of("table", tableName, "database", name));
        }

        // Execute the command directly on the table
        command.execute(this);
    }

    /**
     * Execute a SQL update statement
     *
     * @param sql SQL statement to execute
     * @param params Parameters for the SQL statement
     * @return Number of rows affected
     * @throws DatabaseException if execution fails
     */
    public int executeUpdate(String sql, Object... params) {
        // For the in-memory implementation, this is a stub
        // In a real database, this would execute the SQL
        return 0;
    }
}
