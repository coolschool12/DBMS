package eg.edu.alexu.csd.oop.db.cs30;

import java.util.*;
import java.util.concurrent.CountDownLatch;

public class Table {
    private HashMap<String,Integer> map=new HashMap<>();
    private String[] columnNames;
    private ArrayList<Row> rows;
    private String tableName;
    //specify column names and types
    public Table (String[] columnNames,Integer[] columnTypes){
        rows=new ArrayList<Row>();
        this.columnNames=columnNames;
        for(int i=0;i<columnNames.length;i++){
            map.put(columnNames[i],columnTypes[i]);
        }
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    // insert all values of columns in order
    public int insertRow(Object[] values){
        return insertRow(this.columnNames,values);
    }
    // insert some of columns values (others will be null)
    public int insertRow(String[] columnNames, Object[] values) throws RuntimeException{
        this.checkColumns(columnNames);
        this.checkTypes(columnNames,values);
        Row neo=new Row(columnNames,values);
        rows.add(neo);
        return 1;
    }
    // select all table
    public Object[][] select(){
        return select(this.columnNames);
    }
    // select some of table's columns
    public Object[][] select(String[] columnNames) throws RuntimeException{
        ArrayList<Integer> selectedRows=new ArrayList<Integer>();
        for(int i=0;i<rows.size();i++)
            selectedRows.add(i);
        return SelectFromRows(columnNames,selectedRows);
    }
    // select all columns and some of table's rows (rows selection depend on the condition)
    public Object[][] select(String ColumnName,char Operator,Object value) throws RuntimeException{
        return select(this.columnNames,ColumnName,Operator,value);
    }
    // select some of table's columns and some of table's rows (rows selection depend on the condition)
    public Object[][] select(String[] columnNames,String ColumnName,char Operator,Object value) throws RuntimeException{
        this.checkCondition(ColumnName,value);
        Integer typeOfColumn=map.get(ColumnName);
        ArrayList<Integer> selectedRows=new ArrayList<Integer>();
        for(int i=0;i<rows.size();i++){
            if(rows.get(i).Condition(ColumnName,Operator,value,typeOfColumn))
                selectedRows.add(i);
        }
        return SelectFromRows(columnNames,selectedRows);
    }
    private Object[][] SelectFromRows(String[] columnNames,ArrayList<Integer> selectedRows) throws RuntimeException{
        this.checkColumns(columnNames);
        Object [][] result=new Object[selectedRows.size()][columnNames.length];
        for(int i=0;i<selectedRows.size();i++) {
            int index=selectedRows.get(i);
            result[i]=rows.get(index).getRow(columnNames);
        }
        return result;
    }
    // Delete some of table's rows (rows selection depend on the condition)
    public int delete(String ColumnName,char Operator,Object value) throws RuntimeException{
        this.checkCondition(ColumnName,value);
        Integer typeOfColumn=map.get(ColumnName);
        int counter=0;
        int i=0;
        while (i<rows.size()){
            if(rows.get(i).Condition(ColumnName,Operator,value,typeOfColumn)){
                counter++;
                rows.remove(i);
            }else{
                i++;
            }
        }
        return counter;
    }
    // update some of columns(columnNames) with certain (values) for some rows (rows selection depend on the condition)
    public int update(String[] columnNames,Object[] values,String ColumnName,char Operator,Object value) throws RuntimeException{
        this.checkCondition(ColumnName,value);
        this.checkColumns(columnNames);
        this.checkTypes(columnNames,values);
        Integer typeOfColumn=map.get(ColumnName);
        int counter=0;
        for(int i=0;i<rows.size();i++){
            Row nowRow=rows.get(i);
            if(nowRow.Condition(ColumnName,Operator,value,typeOfColumn)){
                counter++;
                nowRow.updateRow(columnNames,values);
            }
        }
        return counter;
    }
    // update some of columns(columnNames) with certain (values) for all rows
    public int update(String[] columnNames,Object[] values) throws RuntimeException{
        this.checkColumns(columnNames);
        this.checkTypes(columnNames,values);
        for(int i=0;i<rows.size();i++)
            rows.get(i).updateRow(columnNames,values);
        return rows.size();
    }
    private void checkTypes(String[] columnNames,Object[] values) throws RuntimeException{
        //check same size
        if(columnNames.length!=values.length) {
            throw new RuntimeException("diffrenet array lengthes "+columnNames.length +" " +values.length);
        }
        //check correct values types
        for(int i=0;i<columnNames.length;i++){
            if(values[i] instanceof Integer){
                if(this.map.get(columnNames[i])!=1)
                    throw new RuntimeException("Invalid type for "+columnNames[i]);
            }else if(values[i] instanceof String){
                if(this.map.get(columnNames[i])!=0)
                    throw new RuntimeException("Invalid type for "+columnNames[i]);
            }else{
                throw new RuntimeException("Invalid type for "+columnNames[i]);
            }
        }
    }
    private void checkColumns(String[] columnNames) throws RuntimeException{
        //check duplication in columnNames
        Set<String> hash_Set = new HashSet<String>(Arrays.asList(columnNames));
        if(hash_Set.size()!=columnNames.length)
            throw new RuntimeException("duplication in columnNames");
        // check correct names
        for(int i=0;i<columnNames.length;i++) {
            if (map.get(columnNames[i]) == null)
                throw new RuntimeException("No Column with such name ( " +columnNames[i]+" )");
        }
    }
    private void checkCondition(String ColumnName,Object value) throws RuntimeException{
        this.checkColumns(new String[]{ColumnName});
        this.checkTypes(new String[]{ColumnName}, new Object[]{value} );
    }
}