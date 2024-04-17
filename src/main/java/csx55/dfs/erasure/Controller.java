package csx55.dfs.erasure;

public class Controller {
    public static void main (String [] args) {
        //call the main controller in Reed solomon mode
        csx55.dfs.replication.Controller.main(new String[]{ args[0], "Reed-Solomon"});
    }

}
