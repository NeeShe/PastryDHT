package model;

public class SuccessMessage extends Message {
    @Override
    public int getMsgType() {
        return SUCCESS_MSG;
    }
}
