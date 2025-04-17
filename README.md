

## 项目结构

```text
src/
├── main/
│   ├── java/
│   │   ├── core/
│   │   │   ├── DatabaseEngine.java       # 主入口
│   │   │   ├── metadata/
│   │   │   │   ├── Database.java         # 数据库对象
│   │   │   │   ├── Table.java            # 表结构定义
│   │   │   │   └── Column.java           # 列定义
│   │   │   ├── storage/
│   │   │   │   ├── JSONStorageEngine.java # JSON存储引擎
│   │   │   │   └── FileSystemManager.java # 文件操作
│   │   │   ├── parser/
│   │   │   │   ├── DDLParser.java        # SQL解析器
│   │   │   │   └── Tokenizer.java        # 词法分析
│   │   │   └── exception/
│   │   │       ├── DatabaseException.java
│   │   │       └── SyntaxException.java
│   │   └── demo/
│   │       └── DemoRunner.java           # 演示用例
│   └── resources/                        # 存储数据文件
└── test/                                 # 单元测试
```

