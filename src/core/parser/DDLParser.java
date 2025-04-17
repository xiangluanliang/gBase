package core.parser;

/**
 * ProjectName: gBase
 * ClassName: DDLParser
 * Package : core.parser
 * Description:
 *
 * @Author Lxl
 * @Create 2025/4/17 16:14
 * @Version 1.0
 */

import core.metadata.*;
import core.exception.ParseException;

import java.sql.JDBCType;
import java.util.*;
import java.util.stream.Collectors;

/**
 * DDL 语法解析器
 * 功能：
 * 1. 解析CREATE TABLE语句
 * 2. 解析ALTER TABLE语句
 * 3. 生成表结构对象
 */
public class DDLParser {
    private final Tokenizer tokenizer;
    private List<Tokenizer.Token> tokens;
    private int currentPos;

    public DDLParser() {
        this.tokenizer = new Tokenizer();
    }

    /**
     * 解析DDL语句生成Table对象
     * @param ddl SQL DDL语句
     * @return 解析后的Table对象
     */
    public Table parseCreateTable(String ddl) {
        this.tokens = tokenizer.tokenize(ddl);
        this.currentPos = 0;

        expectKeyword("CREATE");
        expectKeyword("TABLE");

        // 处理可选的schema前缀
        String tableName = parseIdentifier();
        if (peekSymbol(".")) {
            String schema = tableName;
            consumeSymbol(".");
            tableName = parseIdentifier();
            return new Table(tableName, schema, parseColumnDefinitions());
        } else {
            return new Table(tableName, null, parseColumnDefinitions());
        }
    }

    // 解析列定义部分
    private List<Column> parseColumnDefinitions() {
        expectSymbol("(");
        List<Column> columns = new ArrayList<>();

        while (!peekSymbol(")")) {
            columns.add(parseColumnDefinition());
            if (!peekSymbol(")")) {
                expectSymbol(",");
            }
        }

        expectSymbol(")");
        return columns;
    }

    // 解析单个列定义
    private Column parseColumnDefinition() {
        String colName = parseIdentifier();
        Column.Builder builder = Column.builder(colName, parseDataType());

        // 解析列约束
        while (true) {
            if (peekKeyword("PRIMARY")) {
                consumeKeyword("PRIMARY");
                consumeKeyword("KEY");
                builder.addConstraint(Column.Constraint.PRIMARY_KEY);
            } else if (peekKeyword("NOT")) {
                consumeKeyword("NOT");
                consumeKeyword("NULL");
                builder.addConstraint(Column.Constraint.NOT_NULL);
            } else if (peekKeyword("UNIQUE")) {
                consumeKeyword("UNIQUE");
                builder.addConstraint(Column.Constraint.UNIQUE);
            } else if (peekKeyword("DEFAULT")) {
                consumeKeyword("DEFAULT");
                builder.defaultValue(parseDefaultValue());
            } else {
                break;
            }
        }

        return builder.build();
    }

    // 解析数据类型
    private JDBCType parseDataType() {
        String typeName = consume(Tokenizer.TokenType.KEYWORD).value.toUpperCase();

        // 处理带长度的类型 (VARCHAR(255))
        if (peekSymbol("(")) {
            consumeSymbol("(");
            int length = Integer.parseInt(consume(Tokenizer.TokenType.NUMBER).value);

            if (peekSymbol(",")) {
                consumeSymbol(",");
                int scale = Integer.parseInt(consume(Tokenizer.TokenType.NUMBER).value);
                consumeSymbol(")");
                return parseComplexType(typeName, length, scale);
            } else {
                consumeSymbol(")");
                return parseTypeWithLength(typeName, length);
            }
        }

        return JDBCType.valueOf(typeName);
    }

    private JDBCType parseTypeWithLength(String type, int length) {
        switch (type) {
            case "VARCHAR":
            case "CHAR":
                return JDBCType.valueOf(type);
            default:
                throw new ParseException("Unsupported type with length: " + type);
        }
    }

    private JDBCType parseComplexType(String type, int precision, int scale) {
        if ("DECIMAL".equals(type) || "NUMERIC".equals(type)) {
            return JDBCType.DECIMAL;
        }
        throw new ParseException("Unsupported complex type: " + type);
    }

    // === 解析工具方法 ===
    private String parseIdentifier() {
        Tokenizer.Token token = consume(Tokenizer.TokenType.IDENTIFIER);
        return token.value;
    }

    private String parseDefaultValue() {
        if (peek(Tokenizer.TokenType.STRING_LITERAL)) {
            String value = consume(Tokenizer.TokenType.STRING_LITERAL).value;
            return value.substring(1, value.length() - 1); // 去掉引号
        } else if (peek(Tokenizer.TokenType.NUMBER)) {
            return consume(Tokenizer.TokenType.NUMBER).value;
        } else if (peekKeyword("NULL")) {
            consumeKeyword("NULL");
            return null;
        }
        throw new ParseException("Expected default value");
    }

    // === Token消费方法 ===
    private Tokenizer.Token consume(Tokenizer.TokenType type) {
        if (currentPos >= tokens.size()) {
            throw new ParseException("Unexpected end of input");
        }
        Tokenizer.Token token = tokens.get(currentPos);
        if (token.type != type) {
            throw new ParseException("Expected " + type + " but found " + token.type);
        }
        currentPos++;
        return token;
    }

    private void consumeKeyword(String keyword) {
        Tokenizer.Token token = consume(Tokenizer.TokenType.KEYWORD);
        if (!token.value.equalsIgnoreCase(keyword)) {
            throw new ParseException("Expected keyword " + keyword);
        }
    }

    private void consumeSymbol(String symbol) {
        Tokenizer.Token token = consume(Tokenizer.TokenType.SYMBOL);
        if (!token.value.equals(symbol)) {
            throw new ParseException("Expected symbol " + symbol);
        }
    }

    private void expectKeyword(String keyword) {
        if (!peekKeyword(keyword)) {
            throw new ParseException("Expected keyword: " + keyword);
        }
        consumeKeyword(keyword);
    }

    private void expectSymbol(String symbol) {
        if (!peekSymbol(symbol)) {
            throw new ParseException("Expected symbol: " + symbol);
        }
        consumeSymbol(symbol);
    }

    // === Token前瞻方法 ===
    private boolean peek(Tokenizer.TokenType type) {
        return currentPos < tokens.size() &&
                tokens.get(currentPos).type == type;
    }

    private boolean peekKeyword(String keyword) {
        return currentPos < tokens.size() &&
                tokens.get(currentPos).type == Tokenizer.TokenType.KEYWORD &&
                tokens.get(currentPos).value.equalsIgnoreCase(keyword);
    }

    private boolean peekSymbol(String symbol) {
        return currentPos < tokens.size() &&
                tokens.get(currentPos).type == Tokenizer.TokenType.SYMBOL &&
                tokens.get(currentPos).value.equals(symbol);
    }
}
