package eg.edu.alexu.csd.oop.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

public class Table {
    private HashMap<String,Integer> map=new HashMap<>();
    private String[] columnNames;
    private ArrayList<Row> rows;
    private String tableName;

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

    public int insertRow(String[] columnNames, Object[] values){
        Row neo=new Row(this.columnNames,columnNames,values);
        rows.add(neo);
        return 1;
    }
    public Object[][] selectAll(){
        return select(this.columnNames);
    }
    public Object[][] select(String[] columnNames){
        ArrayList<Integer> selectedRows=new ArrayList<Integer>();
        for(int i=0;i<rows.size();i++)
            selectedRows.add(i);
        return SelectFromRows(columnNames,selectedRows);
    }
    public Object[][] selectCondition(String[] columnNames,String ColumnName,char Operator,Object value){
        Integer typeOfColumn=map.get(ColumnName);
        ArrayList<Integer> selectedRows=new ArrayList<Integer>();
        for(int i=0;i<rows.size();i++){
            if(rows.get(i).Condition(ColumnName,Operator,value,typeOfColumn))
                selectedRows.add(i);
        }
        return SelectFromRows(columnNames,selectedRows);
    }
    private Object[][] SelectFromRows(String[] columnNames,ArrayList<Integer> selectedRows){
        Object [][] result=new Object[selectedRows.size()][columnNames.length];
        for(int i=0;i<selectedRows.size();i++) {
            int index=selectedRows.get(i);
            result[i]=rows.get(index).getRow(columnNames);
        }
        return result;
    }

    public int deleteCondition(String ColumnName,char Operator,Object value){
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
    public int updateCondition(String[] columnNames,Object[] values,String ColumnName,char Operator,Object value){
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
}
