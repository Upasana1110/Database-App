package db.assignment.pkg4;

import java.sql.*;
import java.util.*;
import java.io.*;

/**
 *
 * @author Upasana
 */
public class DBAssignment4 {

    static Connection conn = null;
    static ArrayList<String> databases;
    static String currentDatabase = "";
    static int currentDatabaseIndex = -1;
    //Place the DBFiles folder in C:
    static String dumpPath = "C:\\DBFiles\\dumpFiles\\";
    static String sqlPath = "C:/DBFiles/queryFiles/";
    static String databaseFilePath = "C:/DBFiles/database.txt";
    // Please replace with mySQLDump Path in the local machine.
    static String mySqlDumpPath = "C:/Program Files/MySQL/MySQL Workbench 6.3 CE/mysqldump";

    public static void main(String[] args) throws Exception,
            IOException, SQLException {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("Could not load the driver");
        }

        boolean done = false;
        do {
            printMenu();
            System.out.print("Type in your option: ");
            System.out.flush();
            String ch = readLine();
            System.out.println();

            switch (ch.charAt(0)) {
                case 'a':
                    ConnectToADatabase();
                    break;
                case 'b':
                    showAndDescribeTables();
                    break;
                case 'c':
                    runAQuery();
                    break;
                case 'd':
                    dumpDatabase();
                    break;
                case 'e':
                    Settings();
                    break;
                case 'q':
                    done = true;
                    break;
                default:
                    System.err.println(" ERROR: Not a valid option ");
            } //switch
        } while (!done);
    }

    private static String readEntry(String prompt) {
        try {
            StringBuffer buffer = new StringBuffer();
            System.out.print(prompt);
            System.out.flush();
            int c = System.in.read();
            while (c != '\n' && c != 1) {
                buffer.append((char) c);
                c = System.in.read();
            }
            return buffer.toString().trim();
        } catch (IOException e) {
            System.err.println(e.getMessage());
            return "";
        }
    }

    private static String readLine() {
        InputStreamReader isr = new InputStreamReader(System.in);
        BufferedReader br = new BufferedReader(isr, 1);
        String line = "";
        try {
            line = br.readLine();
        } catch (IOException e) {
            System.out.println("Error in SimpleIO.readLine: "
                    + "IOException was thrown");
            System.exit(1);
        }
        return line;
    }

    private static void printMenu() {
        System.out.println("\n Select an option: ");
        System.out.println("(a) Connect to a database ");
        System.out.println("(b) Show and Describe Tables ");
        System.out.println("(c) Run a query ");
        System.out.println("(d) Dump a database ");
        System.out.println("(e) Settings ");
        System.out.println("(q) Quit \n");
    }

    static void ConnectToADatabase() {
        Scanner scan = null;
        try {
            File databaseFile = new File(databaseFilePath);
            scan = new Scanner(databaseFile);
            printConnectDatabaseMenu(scan);
        } catch (Exception e) {
            System.err.println("ERROR: Please correct the path of the databaseFile.");
            System.out.println();
            System.out.println("1. New Database ");
//			return;
        }

        System.out.print("Type in your option: ");
        System.out.flush();
        String ch = readLine();
        System.out.println();

        switch (ch.charAt(0)) {
            case '1':
                String databaseDetails = "";
                String name = readEntry("Enter Database Name: ");
                String IP = readEntry("Enter IP Number of database Server (Default 127.0.0.1): ").trim();
                String Port = readEntry("Enter Port (Default 3306): ").trim();
                String Username = readEntry("Enter Username: ").trim();
                String Password = readEntry("Enter Password: ").trim();
                databaseDetails = name + ";" + IP + ";" + Port + ";" + Username + ";" + Password;
                try {
                    conn = DriverManager.getConnection(("jdbc:mysql://" + IP + "/" + name), Username, Password);
                    currentDatabase = name;
                    if (!databases.isEmpty()) {
                        currentDatabaseIndex = databases.size() + 1;
                    } else {
                        currentDatabaseIndex = 0;
                    }
                    databases.add(databaseDetails);
                    System.out.println(databases.toString());
                    System.out.println("currentDatabaseIndex: " + currentDatabaseIndex);

                    FileWriter fileWriter = new FileWriter(databaseFilePath, true);
                    BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
                    PrintWriter printWriter = new PrintWriter(bufferedWriter);
                    printWriter.println("");
                    printWriter.println(databaseDetails);
                    printWriter.close();
                    bufferedWriter.close();
                    fileWriter.close();
                } catch (IOException e) {
                    System.err.println("ERROR: Please correct the path of the databaseFile to save the database in memory.");
                    return;
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                    return;
                }

                break;

            default:
                int index = -1;
                try {
                    index = Integer.parseInt(ch.charAt(0) + "");
                } catch (Exception e) {
                    System.err.println("Enter valid option.");
                    break;
                }
                if (databases.size() >= index) {
                    String details[] = databases.get(index).split(";");
                    currentDatabaseIndex = Integer.parseInt(ch.charAt(0) + "");

                    try {
                        conn = DriverManager.getConnection(("jdbc:mysql://" + details[1] + "/" + details[0]), details[3], details[4]);
                        currentDatabase = details[0];
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                }
                break;

        }

    }

    private static void Settings() {
        System.out.println("Choose an option: ");
        System.out.println("a. Set SQL File Location: ");
        System.out.println("b. Set Dump File Location: ");
        System.out.print("Type in your option: ");
        System.out.flush();
        String ch = readLine();
        System.out.println();

        switch (ch.charAt(0)) {
            case 'a':
                sqlPath = readEntry("Enter valid SQL Path followed by '/'(eg: 'C:/queryFiles/')");
                break;
            case 'b':
                dumpPath = readEntry("Enter valid Dump file path followed by '/'(eg: 'C:/dumpFiles/')");
                break;
            default:
                System.err.println("ERROR: Invalid choice");
        }
    }

    static void printConnectDatabaseMenu(Scanner scan) {

        System.out.println("\n Select an option: ");
        int counter = 2;
        databases = new ArrayList<String>();
        databases.add("none");
        databases.add("none");
        System.out.println("1. New Database ");

        while (scan.hasNext()) {
            String databaseDetails = scan.nextLine();
            String databaseName = databaseDetails.split(";")[0];
            System.out.println(counter + ". " + databaseName);
            databases.add(databaseDetails);
            counter++;
        }
        scan.close();

    }

    static void showAndDescribeTables() {

        if (conn != null) {
            String sqlString = "show tables";
            Statement stmt = null;
            try {
                System.out.println("Tables in this database: ");
                stmt = conn.createStatement();
                ResultSet r = stmt.executeQuery(sqlString);
                String tableName = "";
                while (r.next()) {
                    tableName = r.getString(1);
                    System.out.println("* " + tableName);
                }

                tableName = readEntry("Enter a tablename to see its structure: ");
                sqlString = "desc " + tableName;
                r = stmt.executeQuery(sqlString);
                System.out.println("| Field" + "| \t\t" + "| Type" + "| \t" + "| Null" + "| \t" + "| Key" + "| \t" + "| Default" + "| \t" + "| Extra|");
                while (r.next()) {
                    System.out.print("| " + r.getString(1) + " | \t");
                    System.out.print("| " + r.getString(2) + " | \t");
                    System.out.print("| " + r.getString(3) + " | \t");
                    System.out.print("| " + r.getString(4) + " | \t");
                    System.out.print("| " + r.getString(5) + " | \t");
                    System.out.print("| " + r.getString(6) + "\t|");
                    System.out.println();
                }
            } catch (SQLException e) {
                System.err.println("ERROR: Invalid Table Name");
                showAndDescribeTables();
            }
        } else {
            System.err.println("ERROR: Please connect to a database first.");
        }
    }

    static void runAQuery() {

        if (conn != null) {

            Statement stmt = null;

            try {
                stmt = conn.createStatement();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            System.out.println("Choose an option: ");
            System.out.println("a. Run Single Query: ");
            System.out.println("b. Run SQL File: ");
            System.out.flush();
            System.out.print("Type in your option: ");
            String ch = readLine();
            System.out.println();

            try {
                switch (ch.charAt(0)) {
                    case 'a':
                        String sqlString = readEntry("Enter a valid SQL Query: ");
                        ResultSet r = stmt.executeQuery(sqlString);
                        ResultSetMetaData metadata = r.getMetaData();
                        int columnCount = metadata.getColumnCount();
                        for (int i = 1; i <= columnCount; i++) {
                            System.out.print("| " + metadata.getColumnName(i) + " | \t");
                        }
                        System.out.println();
                        while (r.next()) {
                            String row = "";
                            for (int i = 1; i <= columnCount; i++) {
                                row += r.getString(i) + ", ";
                            }
                            System.out.println("| "  + row + " | \t");
                        }
                        break;
                    case 'b':
                        String fileName = readEntry("Enter file name: ");
                        File f = new File(sqlPath + fileName);
                        Scanner scan = new Scanner(f);
                        while (scan.hasNext()) {
                            sqlString = scan.nextLine();
                            r = stmt.executeQuery(sqlString);
                            metadata = r.getMetaData();
                            columnCount = metadata.getColumnCount();
                            for (int i = 1; i <= columnCount; i++) {
                                System.out.print("| " + metadata.getColumnName(i) + " | \t");
                            }
                            System.out.println();
                            while (r.next()) {
                                String row = "";
                                for (int i = 1; i <= columnCount; i++) {
                                    row += r.getString(i) + ", ";
                                }
                                System.out.println("| "  + row + " | \t");
                            }
                            System.out.println();
                        }
                        break;
                    default:
                        System.err.println("ERROR: Invalid choice");
                        break;
                }
            } catch (SQLException e) {
                System.err.println(e.getMessage());
                System.err.println("ERROR: Invalid Query");
            } catch (Exception e) {
                System.out.println("ERROR: Invalid Path");
            }
        } else {
            System.err.println("ERROR: Please connect to a database first.");
        }
    }

    static void dumpDatabase() {
        if (currentDatabaseIndex != -1) {
            String username = databases.get(currentDatabaseIndex).split(";")[3];
            String password = databases.get(currentDatabaseIndex).split(";")[4];

            ArrayList<File> dumpFiles = findAllDumpFiles(dumpPath);
            int counter = 1;
            if (dumpFiles != null) {
                for (File f : dumpFiles) {
                    System.out.println((counter++) + ". " + f.getName());
                }
            }
            String fileName = readEntry("Enter filename (without .sql): ");
            while (dumpFiles.contains(dumpPath+fileName+".sql")) {
                System.out.println("File already Exists, Please choose a different file name.");
                fileName = readEntry("Enter filename (without .sql): ");
            }

            try {
                //Please replace with local path.
                String command = mySqlDumpPath + " -u" + username + " -p" + password + " " + currentDatabase + " > " + dumpPath + fileName + ".sql";
                Runtime.getRuntime().exec(command);
                System.out.println("The file was dumped Successfully at: " + dumpPath + "with file name: " + fileName + ".sql");
            } catch (Exception e) {
                System.err.println(e.getMessage());
                System.err.println("Dump Failed.");
            }
        } else {
            System.err.println("ERROR: Please connect to a database first.");
        }
    }

    public static ArrayList<File> findAllDumpFiles(String dirName) {
        File dir = new File(dirName);

        File[] files = dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String filename) {
                return filename.endsWith(".sql");
            }
        });
        ArrayList<File> allFiles = new ArrayList<File>();
        for (File f : files) {
            allFiles.add(f);
        }
        return allFiles;
    }

}
