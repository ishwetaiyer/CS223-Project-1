package cs223.project1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class FileParser {
    private static final int x = "'2017-11-0800:00:00',".length();
    private static Scanner scan = new Scanner(System.in);
    static List<Transaction> createTransactions(Connection conn, String fileName, final int batchSize) {
        String currentTimestamp = "";
        Transaction currentTransaction = new Transaction();
        int batchCounter = 0;
        List<Transaction> transactionList = new LinkedList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                CustomTimestampStatement cts = readCustomStatement(conn, line);
                if (currentTimestamp.equals(cts.timestamp) && batchCounter < batchSize) {
                    currentTransaction.addStatement(cts.c);
                    batchCounter++;
                } else {
                    batchCounter = 0;
                    currentTimestamp = cts.timestamp;

                    Transaction t = new Transaction();
                    t.addStatement(cts.c);
                    currentTransaction = t;
                    transactionList.add(t);
                    batchCounter++;

                }
            }
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
        System.out.print("Press any key to continue . . . ");
        scan.nextLine();
        return transactionList;
    }

    private static CustomTimestampStatement readCustomStatement(Connection conn, String line) throws SQLException {

        String regex = "('[0-9-:]*'),(.*)";
        Pattern p = Pattern.compile(regex);

        String timestamp = line.substring(0, x);
        String query = line.substring(x);

        CustomStatement c = new CustomStatement(query.startsWith("S"), query);
        return new CustomTimestampStatement(c, timestamp);
    }

    private static class CustomTimestampStatement {
        CustomStatement c;
        String timestamp;

        CustomTimestampStatement(CustomStatement c, String timestamp) {
            this.c = c;
            this.timestamp = timestamp;
        }
    }
}
