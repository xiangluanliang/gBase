package core.exception;

/**
 * ProjectName: gBase
 * ClassName: SyntaxException
 * Package : core.exception
 * Description:
 *
 * @Author Lxl
 * @Create 2025/4/17 16:12
 * @Version 1.0
 */

/**
 * SQL语法异常
 * 用于标识SQL语句中的语法错误，包含错误位置和错误建议
 */
public class SyntaxException extends DatabaseException {
    // 错误位置标记（-1表示未知位置）
    private final int errorPosition;
    // 出错的SQL片段
    private final String errorContext;
    // 预期语法格式
    private final String expectedSyntax;

    /**
     * 基础构造函数
     * @param message 错误描述
     */
    public SyntaxException(String message) {
        this(message, -1, null, null);
    }

    /**
     * 完整构造函数
     * @param message 错误描述
     * @param errorPosition 错误在SQL中的位置（字符偏移量）
     * @param errorContext 错误位置的上下文内容
     * @param expectedSyntax 期望的正确语法示例
     */
    public SyntaxException(String message,
                           int errorPosition,
                           String errorContext,
                           String expectedSyntax) {
        super(formatMessage(message, errorPosition, errorContext, expectedSyntax));
        this.errorPosition = errorPosition;
        this.errorContext = errorContext;
        this.expectedSyntax = expectedSyntax;
    }

    // 格式化错误消息
    private static String formatMessage(String message,
                                        int position,
                                        String context,
                                        String expected) {
        StringBuilder sb = new StringBuilder("Syntax error: ");
        sb.append(message);

        if (position >= 0) {
            sb.append("\n  at position: ").append(position);
        }

        if (context != null) {
            sb.append("\n  near: \"").append(context).append("\"");
        }

        if (expected != null) {
            sb.append("\n  expected: ").append(expected);
        }

        return sb.toString();
    }

    // === 访问方法 ===
    public int getErrorPosition() {
        return errorPosition;
    }

    public String getErrorContext() {
        return errorContext;
    }

    public String getExpectedSyntax() {
        return expectedSyntax;
    }

    /**
     * 快速创建缺失关键字的异常
     * @param keyword 缺失的关键字
     * @param position 错误位置
     * @param context 上下文
     * @return 构造好的异常对象
     */
    public static SyntaxException missingKeyword(String keyword,
                                                 int position,
                                                 String context) {
        return new SyntaxException("Missing required keyword: " + keyword,
                position,
                context,
                "..." + keyword + "...");
    }

    /**
     * 快速创建非法token的异常
     * @param token 非法的token
     * @param position 错误位置
     * @param context 上下文
     * @return 构造好的异常对象
     */
    public static SyntaxException invalidToken(String token,
                                               int position,
                                               String context) {
        return new SyntaxException("Invalid token: " + token,
                position,
                context,
                null);
    }
}

