package csx55.dfs.erasure;

public class Client {
    public static void main (String [] args) {
        //call the main controller in Reed solomon mode
        csx55.dfs.replication.Client.main(new String[]{args[0], args[1]});
    }
}
