package core.storage;

/**
 * ProjectName: gBase
 * ClassName: FileUtil
 * Package : core.storage
 * Description:
 *
 * @Author Lxl
 * @Create 2025/4/17 17:00
 * @Version 1.0
 */

import java.io.*;
import java.nio.file.*;

public class FileUtils {
    public static void copyDirectory(File source, File target) throws IOException {
        if (!target.exists()) {
            target.mkdirs();
        }

        for (File file : source.listFiles()) {
            if (file.isDirectory()) {
                copyDirectory(file, new File(target, file.getName()));
            } else {
                Files.copy(file.toPath(),
                        new File(target, file.getName()).toPath(),
                        StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }
}

