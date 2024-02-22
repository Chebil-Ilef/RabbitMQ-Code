package two_files_dispatching;

import java.io.Serializable;

public class obj implements Serializable {
    private String msg;
    private int prio;

    public int getPrio(){
        return prio;
    }

    public String getMsg(){
        return msg;
    }

    public String toString(){
        return "msg: " + msg + " prio: " + prio;
    }

}
