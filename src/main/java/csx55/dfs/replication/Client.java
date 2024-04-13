package csx55.dfs.replication;

import csx55.dfs.domain.UserCommands;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class Client {
    public static void main (String [] args) {
        try (Socket socketToController = new Socket(args[0], Integer.parseInt(args[1]));
             //try (Socket socketToController = new Socket("localhost", 12341);
             ServerSocket clientSocket = new ServerSocket(0);)
        {
             Client client = new Client();
             Thread userThread = new Thread(() -> client.userInput(client));
             userThread.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }

    /***
     * USER Inputs
     */
    private void userInput (Client node) {
        try {
            boolean running = true;
            while (running) {
                // Scanner scan = new Scanner(System.in);
                System.out.println("***************************************");
                System.out.println("[Messaging Node] Enter your Message Node command");
                System.out.println(UserCommands.clientCommandsToString());
                //String userInput = scan.nextLine();
                BufferedReader inputReader = new BufferedReader(new InputStreamReader(System.in));
                String userInput = inputReader.readLine();
                System.out.println("User input detected " + userInput);
                boolean containsSpace = false,
                        validUploadFilesCmd = false,
                        validDownloadFilesCmd = false,
                        validCheckSuccessorCmd = false;
                String uploadFilePath = ""; //local path to file to be uploaded
                String downloadFileName = "";
                if (userInput.contains(" ")) {
                    containsSpace = true;
                    if (userInput.startsWith(UserCommands.UPLOAD_FILE.getCmd()) ||
                            userInput.toUpperCase().contains("upload") ||
                            userInput.startsWith(String.valueOf(UserCommands.UPLOAD_FILE.getCmdId()))) {
                        validUploadFilesCmd = true;
                        uploadFilePath = userInput.split(" ")[1];
                    } else if (userInput.startsWith(UserCommands.DOWNLOAD_FILE.getCmd()) ||
                            userInput.toUpperCase().contains("download") ||
                            userInput.startsWith(String.valueOf(UserCommands.DOWNLOAD_FILE.getCmdId()))) {
                        validDownloadFilesCmd = true;
                        downloadFileName = userInput.split(" ")[1];
                    }
                }
            }
        }
        catch (Exception ex) {

        }
    }
}
