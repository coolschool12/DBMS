package eg.edu.alexu.csd.oop.db.cs30.jdbc;

public class SelectInfo {
    private Object[][] result;
    private String[] columnNames;
    private Integer[] columnTypes;
    private String tableName;
    public SelectInfo(Object[][] result, String[] columnNames, Integer[] columnTypes, String tableName){
        this.result=result;
        this.columnNames=columnNames;
        this.columnTypes=columnTypes;
        this.tableName=tableName;
    }

    public Object[][] getResult() {
        return result;
    }

    public void setResult(Object[][] result) {
        this.result = result;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(String[] columnNames) {
        this.columnNames = columnNames;
    }

    public Integer[] getColumnTypes() {
        return columnTypes;
    }

    public void setColumnTypes(Integer[] columnTypes) {
        this.columnTypes = columnTypes;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
