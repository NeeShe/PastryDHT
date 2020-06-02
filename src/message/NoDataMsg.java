package message;

public class NoDataMsg extends Message {
    private String msg;

    public NoDataMsg(String msg) {
        this.msg = msg;
    }

    public String getMsg() {
        return msg;
    }

    @Override
    public int getMsgType() {
        return NO_DATA_MSG;
    }
}
