package core.storage;

/**
 * ProjectName: gBase
 * ClassName: FileSystemManager
 * Package : core.parser
 * Description:
 *
 * @Author Lxl
 * @Create 2025/4/17 16:13
 * @Version 1.0
 */
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

/**
 * 数据库文件系统管理
 * 功能：
 * 1. 管理数据库文件的创建/删除
 * 2. 处理表数据文件的读写
 * 3. 处理目录结构和元数据文件
 */
public class FileSystemManager {
    private final Path databaseRoot;

    public FileSystemManager(String databaseName) throws IOException {
        this.databaseRoot = Paths.get("data", databaseName);
        initializeFileSystem();
    }

    /**
     * 初始化文件系统结构
     */
    private void initializeFileSystem() throws IOException {
        // 创建必要目录结构
        Files.createDirectories(databaseRoot);
        Files.createDirectories(getTablesDir());
        Files.createDirectories(getMetadataDir());

        // 初始化元数据文件
        if (!Files.exists(getDatabaseMetadataFile())) {
            Files.write(getDatabaseMetadataFile(),
                    "{}".getBytes(),
                    StandardOpenOption.CREATE);
        }
    }

    // === 目录路径方法 ===
    public Path getTablesDir() {
        return databaseRoot.resolve("tables");
    }

    public Path getMetadataDir() {
        return databaseRoot.resolve("metadata");
    }

    public Path getDatabaseMetadataFile() {
        return getMetadataDir().resolve("database.json");
    }

    public Path getTableMetadataFile(String tableName) {
        return getMetadataDir().resolve(tableName + ".meta.json");
    }

    public Path getTableDataFile(String tableName) {
        return getTablesDir().resolve(tableName + ".data.json");
    }

    // === 文件操作方法 ===
    public void createTableFile(String tableName) throws IOException {
        Path dataFile = getTableDataFile(tableName);
        Path metaFile = getTableMetadataFile(tableName);

        if (Files.exists(dataFile)) {
            throw new FileAlreadyExistsException("Table already exists: " + tableName);
        }

        Files.write(dataFile, "[]".getBytes(), StandardOpenOption.CREATE);
        Files.write(metaFile, "{}".getBytes(), StandardOpenOption.CREATE);
    }

    public void deleteTableFile(String tableName) throws IOException {
        Path dataFile = getTableDataFile(tableName);
        Path metaFile = getTableMetadataFile(tableName);

        Files.deleteIfExists(dataFile);
        Files.deleteIfExists(metaFile);
    }

    public List<String> listTables() throws IOException {
        try (Stream<Path> stream = Files.list(getTablesDir())) {
            return stream
                    .filter(path -> path.toString().endsWith(".data.json"))
                    .map(path -> {
                        String filename = path.getFileName().toString();
                        return filename.substring(0, filename.length() - 10); // 移除.data.json后缀
                    })
                    .collect(Collectors.toList());
        }
    }

    // === 文件内容操作 ===
    public String readFileContent(Path file) throws IOException {
        return new String(Files.readAllBytes(file));
    }

    public void writeFileContent(Path file, String content) throws IOException {
        Files.write(file, content.getBytes(),
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.CREATE);
    }

    public boolean tableExists(String tableName) {
        return Files.exists(getTableDataFile(tableName));
    }

    // === 备份/恢复 ===
    public void backupDatabase(String backupName) throws IOException {
        Path backupDir = databaseRoot.resolveSibling(databaseRoot.getFileName() + "_" + backupName);
        FileUtils.copyDirectory(databaseRoot.toFile(), backupDir.toFile());
    }
}
