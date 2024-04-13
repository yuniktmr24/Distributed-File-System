package dfs.domain;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public enum UserCommands {
    UPLOAD_FILE("upload", 11, Collections.singletonList(NodeType.CLIENT)),

    DOWNLOAD_FILE("download", 12, Collections.singletonList(NodeType.CLIENT));
    private final String cmd;
    private final int cmdId;
    private final List<NodeType> nodeType;
    private UserCommands(String cmd, int cmdId, List <NodeType> nodeType) {
        this.cmd = cmd;
        this.cmdId = cmdId;
        this.nodeType = nodeType;
    }

    public String getCmd() {
        return cmd;
    }

    public int getCmdId() {
        return cmdId;
    }

    public List<NodeType> getNodeType() {
        return nodeType;
    }

    public static List<String> getClientCommands() {
        List <String> cmdGuide = new ArrayList<>();
        for (UserCommands cmd: Arrays.stream(UserCommands.values()).filter(i -> i.getNodeType().contains(NodeType.CLIENT)).collect(Collectors.toList())){
            cmdGuide.add("Command : " + cmd.getCmd() +" "+ " ID: "+ cmd.getCmdId());
        }
        return cmdGuide;
    }


    public static String clientCommandsToString() {
        List<String> cmdList = getClientCommands();
        return String.join("\n", cmdList);
    }
}
