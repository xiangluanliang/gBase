package core.parser;

/**
 * ProjectName: gBase
 * ClassName: Tokenizer
 * Package : core.parser
 * Description:
 *
 * @Author Lxl
 * @Create 2025/4/17 16:14
 * @Version 1.0
 */
import core.exception.ParseException;
import java.util.*;
import java.util.regex.*;
import java.util.stream.Collectors;

/**
 * SQL 词法分析器
 * 功能：
 * 1. 将SQL字符串转换为token序列
 * 2. 处理字符串字面量、数字、标识符等
 * 3. 支持注释跳过
 */
public class Tokenizer {
    // Token类型定义
    public enum TokenType {
        KEYWORD, IDENTIFIER, STRING_LITERAL,
        NUMBER, SYMBOL, WHITESPACE, COMMENT
    }

    // Token定义
    public static class Token {
        public final TokenType type;
        public final String value;
        public final int position;

        public Token(TokenType type, String value, int position) {
            this.type = type;
            this.value = value;
            this.position = position;
        }

        @Override
        public String toString() {
            return String.format("%s(%s)", type, value);
        }
    }

    // 词法规则（按优先级排序）
    private static final List<LexRule> RULES = Arrays.asList(
            new LexRule(TokenType.COMMENT, "--.*|/\\*[\\s\\S]*?\\*/"),
            new LexRule(TokenType.WHITESPACE, "\\s+"),
            new LexRule(TokenType.STRING_LITERAL, "'(?:''|[^'])*'"),
            new LexRule(TokenType.NUMBER, "-?\\d+(?:\\.\\d+)?(?:[eE][+-]?\\d+)?"),
            new LexRule(TokenType.KEYWORD,
                    "(?i)CREATE|TABLE|ALTER|ADD|COLUMN|PRIMARY|KEY|UNIQUE|NOT|NULL|DEFAULT|REFERENCES|FOREIGN|INDEX|CHECK|DROP|RENAME|TO|INT|VARCHAR|CHAR|DATE|TIMESTAMP|BOOLEAN|DECIMAL|BIGINT"),
            new LexRule(TokenType.SYMBOL, "[(),;=<>+\\-*/%]"),
            new LexRule(TokenType.IDENTIFIER, "[a-zA-Z_][a-zA-Z0-9_]*")
    );

    // 词法规则定义
    private static class LexRule {
        final TokenType type;
        final Pattern pattern;

        LexRule(TokenType type, String regex) {
            this.type = type;
            this.pattern = Pattern.compile("^(" + regex + ")");
        }
    }

    /**
     * 将SQL字符串转换为token序列
     * @param sql 输入SQL
     * @return 过滤后的token列表（跳过注释和空格）
     */
    public List<Token> tokenize(String sql) {
        List<Token> tokens = new ArrayList<>();
        int pos = 0;
        String remaining = sql;

        while (!remaining.isEmpty()) {
            boolean matched = false;

            for (LexRule rule : RULES) {
                Matcher matcher = rule.pattern.matcher(remaining);
                if (matcher.find()) {
                    String value = matcher.group(1);
                    tokens.add(new Token(rule.type, value, pos));
                    remaining = remaining.substring(value.length());
                    pos += value.length();
                    matched = true;
                    break;
                }
            }

            if (!matched) {
                throw new ParseException("Unexpected character at position " + pos +
                        ": '" + remaining.charAt(0) + "'");
            }
        }

        // 过滤掉注释和空格
        return tokens.stream()
                .filter(t -> t.type != TokenType.COMMENT && t.type != TokenType.WHITESPACE)
                .collect(Collectors.toList());
    }
}
