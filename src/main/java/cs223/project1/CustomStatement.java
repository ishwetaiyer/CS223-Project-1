package cs223.project1;

import java.sql.PreparedStatement;

/**
 * Used while parsing statements from the workload file
 */
public class CustomStatement {
    private boolean selectType;
    private String statement;

    CustomStatement(boolean selectType, String statement) {
        this.selectType = selectType;
        this.statement = statement;
    }

    boolean isSelectType() {
        return selectType;
    }

    public void setSelectType(boolean selectType) {
        this.selectType = selectType;
    }

    public void setStatement(String statement) {
        this.statement = statement;
    }

    String getStatement() {
        return statement;
    }
}