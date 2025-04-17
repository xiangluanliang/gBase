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
    private Column newColumn; // Used for ADD and MODIFY operations
    private String columnName; // Used for DROP operation or as old column name for MODIFY

    /**
     * Constructor for ADD_COLUMN operation
     * @param tableName The name of the table to alter
     * @param alterType Must be ADD_COLUMN
     * @param newColumn The column to add
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
     * Constructor for DROP_COLUMN operation
     * @param tableName The name of the table to alter
     * @param alterType Must be DROP_COLUMN
     * @param columnName The name of the column to drop
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
     * Constructor for MODIFY_COLUMN operation
     * @param tableName The name of the table to alter
     * @param alterType Must be MODIFY_COLUMN
     * @param columnName The name of the column to modify
     * @param newColumn The new column definition
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
     * Execute the ALTER TABLE command on the specified database
     * @param database The database containing the table to alter
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
        // Check if column already exists
        if (table.getColumnByName(newColumn.getName()) != null) {
            throw new DatabaseException("Column '" + newColumn.getName() + "' already exists in table '" + tableName + "'");
        }
        table.addColumn(newColumn);
    }

    private void executeDropColumn(Table table) {
        // Check if column exists
        if (table.getColumnByName(columnName) == null) {
            throw new DatabaseException("Column '" + columnName + "' does not exist in table '" + tableName + "'");
        }
        table.dropColumn(columnName);
    }

    private void executeModifyColumn(Table table) {
        // Check if column exists
        if (table.getColumnByName(columnName) == null) {
            throw new DatabaseException("Column '" + columnName + "' does not exist in table '" + tableName + "'");
        }

        // If column is being renamed (different name in newColumn)
        if (!columnName.equals(newColumn.getName())) {
            // Check that new name doesn't already exist
            if (table.getColumnByName(newColumn.getName()) != null) {
                throw new DatabaseException("Cannot rename column '" + columnName +
                        "' to '" + newColumn.getName() + "' as the target name already exists");
            }
        }

        table.modifyColumn(columnName, newColumn);
    }

    /**
     * Get the name of the table being altered
     * @return The table name
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Get the type of alteration
     * @return The AlterType enum value
     */
    public AlterType getAlterType() {
        return alterType;
    }

    /**
     * Get the column name for DROP operations or old column name for MODIFY operations
     * @return The column name
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * Get the new column definition for ADD or MODIFY operations
     * @return The Column object
     */
    public Column getNewColumn() {
        return newColumn;
    }
}
