package model;

public class ErrorMessage extends Message{
    private String msg;

    public ErrorMessage(String msg) {
        this.msg = msg;
    }

    public String getMsg() {
        return msg;
    }

    @Override
    public int getMsgType() {
        return ERROR_MSG;
    }
}
