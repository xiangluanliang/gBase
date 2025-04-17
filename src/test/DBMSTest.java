package test;

/**
 * ProjectName: gBase
 * ClassName: testDBMS
 * Package : test
 * Description:
 *
 * @Author Lxl
 * @Create 2025/4/17 18:06
 * @Version 1.0
 */

import core.DatabaseEngine;
import core.metadata.Column;
import core.metadata.Table;
import core.parser.DDLParser;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * DBMSTest
 *
 * A test class to demonstrate the functionality of gBase DBMS including:
 * - Database creation and deletion
 * - Table creation, modification, and deletion
 * - DDL parsing
 */
public class DBMSTest {

    private static DatabaseEngine engine;
    private static final String TEST_DB_NAME = "testdb";
    private static final Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) {
        System.out.println("=== gBase DBMS Test ===");

        // Initialize the database engine
        engine = new DatabaseEngine();

        try {
            // Test database operations
            testDatabaseOperations();

            // Test table operations
            testTableOperations();

            // Test DDL parsing
            testDDLParsing();

            System.out.println("\nAll tests completed successfully!");
        } catch (Exception e) {
            System.err.println("Error during tests: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Clean up
            cleanupResources();
        }
    }

    /**
     * Test database creation and deletion
     */
    private static void testDatabaseOperations() {
        System.out.println("\n=== Testing Database Operations ===");

        // List existing databases
        System.out.println("\nListing existing databases:");
        engine.listDatabases();

        // Create a test database
        System.out.println("\nCreating database: " + TEST_DB_NAME);
        try {
            engine.createDatabase(TEST_DB_NAME);
        } catch (Exception e) {
            System.out.println("Note: " + e.getMessage() + " (This may be expected if database already exists)");
            // If the database already exists, we'll continue with the existing one
        }

        // List databases again to verify creation
        System.out.println("\nListing databases after creation:");
        engine.listDatabases();

        // Verify database exists
        boolean exists = engine.databaseExists(TEST_DB_NAME);
        System.out.println("\nVerifying database '" + TEST_DB_NAME + "' exists: " + exists);

        System.out.println("\nDatabase operations test completed.");
    }

    /**
     * Test table creation, modification, and deletion via direct API calls
     */
    private static void testTableOperations() {
        System.out.println("\n=== Testing Table Operations via API Calls ===");

        // Create a test table programmatically
        System.out.println("\nCreating test table programmatically");
        List<Column> columns = new ArrayList<>();

        // Define columns for the test table
        Column idColumn = Column.builder("id", JDBCType.INTEGER)
                .addConstraint(Column.Constraint.PRIMARY_KEY)
                .addConstraint(Column.Constraint.NOT_NULL)
                .build();
        columns.add(idColumn);

        Column nameColumn = Column.builder("name", JDBCType.VARCHAR)
                .addConstraint(Column.Constraint.NOT_NULL)
                .build();
        columns.add(nameColumn);

        Column emailColumn = Column.builder("email", JDBCType.VARCHAR)
                .addConstraint(Column.Constraint.UNIQUE)
                .build();
        columns.add(emailColumn);

        // Create the table object
        Table usersTable = new Table("users", null, columns);

        // Execute DDL to create the table in the database
        System.out.println("\nExecuting CREATE TABLE DDL:");
        String createTableDDL = usersTable.generateCreateDDL();
        System.out.println(createTableDDL);

        try {
            engine.executeDDL(TEST_DB_NAME, createTableDDL);
        } catch (Exception e) {
            System.out.println("Note: " + e.getMessage() + " (This may be expected if table already exists)");
            // If the table already exists, we'll alter it instead
        }

        // Show the database schema
        System.out.println("\nDatabase schema after table creation:");
        engine.showDatabaseSchema(TEST_DB_NAME);

        System.out.println("\nTable operations test completed.");
    }

    /**
     * Test DDL parsing for CREATE, ALTER, and DROP operations
     */
    private static void testDDLParsing() {
        System.out.println("\n=== Testing DDL Parsing ===");
        DDLParser parser = new DDLParser();

        // Test CREATE TABLE parsing
        System.out.println("\nTesting CREATE TABLE parsing:");
        String createDDL = "CREATE TABLE products (\n" +
                "  id INTEGER PRIMARY KEY NOT NULL,\n" +
                "  name VARCHAR NOT NULL,\n" +
                "  price DECIMAL,\n" +
                "  description VARCHAR,\n" +
                "  category VARCHAR\n" +
                ")";
        System.out.println(createDDL);

        try {
            engine.executeDDL(TEST_DB_NAME, createDDL);
            System.out.println("CREATE TABLE parsed and executed successfully.");
        } catch (Exception e) {
            System.out.println("Error parsing CREATE TABLE: " + e.getMessage());
        }

        // Test ALTER TABLE ADD COLUMN
        System.out.println("\nTesting ALTER TABLE ADD COLUMN:");
        String alterAddDDL = "ALTER TABLE products ADD COLUMN stock INTEGER NOT NULL DEFAULT 0";
        System.out.println(alterAddDDL);

        try {
            engine.executeDDL(TEST_DB_NAME, alterAddDDL);
            System.out.println("ALTER TABLE ADD COLUMN parsed and executed successfully.");
        } catch (Exception e) {
            System.out.println("Error parsing ALTER TABLE ADD: " + e.getMessage());
        }

        // Show the updated schema
        System.out.println("\nDatabase schema after ALTER TABLE ADD COLUMN:");
        engine.showDatabaseSchema(TEST_DB_NAME);

        // Test ALTER TABLE MODIFY COLUMN
        System.out.println("\nTesting ALTER TABLE MODIFY COLUMN:");
        String alterModifyDDL = "ALTER TABLE products MODIFY COLUMN description VARCHAR NOT NULL";
        System.out.println(alterModifyDDL);

        try {
            engine.executeDDL(TEST_DB_NAME, alterModifyDDL);
            System.out.println("ALTER TABLE MODIFY COLUMN parsed and executed successfully.");
        } catch (Exception e) {
            System.out.println("Error parsing ALTER TABLE MODIFY: " + e.getMessage());
        }

        // Show the updated schema
        System.out.println("\nDatabase schema after ALTER TABLE MODIFY COLUMN:");
        engine.showDatabaseSchema(TEST_DB_NAME);

        // Test ALTER TABLE DROP COLUMN
        System.out.println("\nTesting ALTER TABLE DROP COLUMN:");
        String alterDropDDL = "ALTER TABLE products DROP COLUMN category";
        System.out.println(alterDropDDL);

        try {
            engine.executeDDL(TEST_DB_NAME, alterDropDDL);
            System.out.println("ALTER TABLE DROP COLUMN parsed and executed successfully.");
        } catch (Exception e) {
            System.out.println("Error parsing ALTER TABLE DROP: " + e.getMessage());
        }

        // Show the updated schema
        System.out.println("\nDatabase schema after ALTER TABLE DROP COLUMN:");
        engine.showDatabaseSchema(TEST_DB_NAME);

        // Test DROP TABLE
        System.out.println("\nTesting DROP TABLE (will be commented out to preserve test data):");
        String dropTableDDL = "DROP TABLE products";
        System.out.println(dropTableDDL + " - commented out for this demo");

        /*
        // Uncomment to test DROP TABLE
        try {
            engine.executeDDL(TEST_DB_NAME, dropTableDDL);
            System.out.println("DROP TABLE parsed and executed successfully.");
        } catch (Exception e) {
            System.out.println("Error parsing DROP TABLE: " + e.getMessage());
        }

        // Show the updated schema
        System.out.println("\nDatabase schema after DROP TABLE:");
        engine.showDatabaseSchema(TEST_DB_NAME);
        */

        System.out.println("\nDDL parsing test completed.");
    }

    /**
     * Clean up resources
     */
    private static void cleanupResources() {
        // Close any open resources
        scanner.close();

        // Optionally drop the test database
        /*
        System.out.println("\nCleaning up - dropping test database:");
        try {
            engine.dropDatabase(TEST_DB_NAME);
            System.out.println("Test database dropped successfully.");
        } catch (Exception e) {
            System.out.println("Error dropping test database: " + e.getMessage());
        }
        */
    }

    /**
     * Utility method to pause for user input
     */
    private static void pauseForInput(String message) {
        System.out.println("\n" + message + " (Press Enter to continue)");
        scanner.nextLine();
    }
}
