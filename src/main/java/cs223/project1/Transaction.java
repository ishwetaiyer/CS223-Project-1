package cs223.project1;

import java.util.ArrayList;
import java.util.List;

public class Transaction {

    private List<CustomStatement> statements;

    public Transaction() {
        statements = new ArrayList<>();
    }

    void addStatement(CustomStatement c) {
        statements.add(c);
    }

    List<CustomStatement> getStatements() {
        return statements;
    }

    public static void main(String[] args) {
        String x = "001";
        System.out.println(Integer.parseInt(x));
    }
}
