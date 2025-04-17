package core.exception;

/**
 * ProjectName: gBase
 * ClassName: ParserException
 * Package : core.exception
 * Description:
 *
 * @Author Lxl
 * @Create 2025/4/17 16:55
 * @Version 1.0
 */

/**
 * SQL解析异常
 */
public class ParseException extends RuntimeException {
    private final int position;

    public ParseException(String message, int position) {
        super(message + (position >= 0 ? " (position: " + position + ")" : ""));
        this.position = position;
    }

    public int getPosition() {
        return position;
    }

    // 为不需要位置信息的异常提供便捷构造方法
    public ParseException(String message) {
        this(message, -1);
    }
}

