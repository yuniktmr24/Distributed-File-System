//package dfs.monitor;
//
//import dfs.replication.Controller;
//
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//
//public class ChunkServerMonitor implements Runnable{
//    private Map<String, Integer> cachedHBInfo = new ConcurrentHashMap<>();
//
//    @Override
//    public void run() {
//        Map <String, Integer> currentHBInfo = Controller.getChunkServerHeartBeatInfo();
//
//        //cold-start problem
//        if (cachedHBInfo.isEmpty()) {
//            cachedHBInfo.putAll(currentHBInfo);
//        }
//        //regular checks for heartbeat counts
//        else {
//            for (String chunkServer : cachedHBInfo.keySet()) {
//                Integer cachedHBCt = cachedHBInfo.get(chunkServer);
//
//                Integer currentHBCt = currentHBInfo.get(chunkServer);
//
//                //if currentHBCt for the node hasn't increased, then there's a problem
//                if (cachedHBCt >= currentHBCt) {
//                    System.out.println("The heartbeat count for "+ chunkServer + " hasn't increased");
//                }
//            }
//        }
//
//    }
//
//}
