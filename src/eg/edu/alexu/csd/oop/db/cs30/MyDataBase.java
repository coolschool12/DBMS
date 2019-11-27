package eg.edu.alexu.csd.oop.db.cs30;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

class MyDataBase {
    private ArrayList<String>tables;
    private String name;
    private String path;
   // private Table activeTable;

    MyDataBase(String name)
    {
      //  activeTable = null;
        this.name = name;
        tables = new ArrayList<>();
    }

    boolean addTable(Object[][] Data, String tableName) throws SQLException
    {

        Table newTable = new Table((String[]) Data[0], (Integer[]) Data[1]);
        TableFactory.createTable(getPath(), getPath(), (String[]) Data[0], (Integer[]) Data[1], tableName);
        newTable.setTableName(tableName);
        tables.add(tableName);
        return true;
    }

    boolean removeTable(String tableName) throws SQLException {
        Table desiredTable = getTheDesiredTable(tableName);

        if (desiredTable == null)
            return false;

        else
            tables.remove(desiredTable.getTableName());

        return true;

    }

    int editTable(Object[][] newContent, String tableName) throws SQLException {

        Table desiredTable = getTheDesiredTable(tableName);

        if (desiredTable == null)
            return 0;
        else
        {
            Object[] values = newContent[0];
            String[] columnNames = (String[]) newContent[1];
            return desiredTable.insertRow(columnNames, values);
        }
    }

    Object[][] select(HashMap<String, Object> properties) throws SQLException {

        Table selectedTable = dealWithTheHash(properties);

        if (properties.get("starflag").equals(1)) {
            if (properties.containsKey("operator"))
                return selectedTable.select((String) properties.get("condColumns"), ((String) properties.get("operator")).charAt(0), properties.get("condValue"));

            else
                return selectedTable.select();
        }
        else {
            String[] columnNames = toStringArray(getColumnsStuff(properties, "selectedColumn"));

            if (properties.containsKey("operator"))
                return  selectedTable.select(columnNames,(String) properties.get("condColumns"),((String) properties.get("operator")).charAt(0), properties.get("condValue"));

            else
                return selectedTable.select(columnNames);
        }

    }

    int delete(HashMap<String, Object> properties) throws SQLException {

        Table selectedTable = dealWithTheHash(properties);

       return selectedTable.delete((String) properties.get("condColumns"),((String) properties.get("operator")).charAt(0), properties.get("condValue"));
    }


    int update(HashMap<String, Object> properties) throws SQLException {

        Table selectedTable = dealWithTheHash(properties);

        String[] columnNames = toStringArray(getColumnsStuff(properties, "selectedColumn"));
        Object[] columnValues = getColumnsStuff(properties, "setValue");

        if (properties.containsKey("operator"))
            return selectedTable.update(columnNames, columnValues,(String) properties.get("condColumns"),((String) properties.get("operator")).charAt(0), properties.get("condValue"));

        else
            return selectedTable.update(columnNames, columnValues);
    }

    String getName() {
        return name;
    }

    String getPath() {
        return path;
    }

    private Table getTheDesiredTable(String tableName) throws SQLException {
        for (String table : tables)
        {
            if (table.equals(tableName)) {
                return TableFactory.loadTable(this.getPath() + "\\" + tableName + ".xml",
                        this.getPath() + "\\" + tableName + ".xsd");
            }
        }
        return null;
    }

    private Object[] getColumnsStuff(HashMap<String, Object> properties, String val)
    {
           ArrayList<Object> columnsStuff = new ArrayList<>();
           long size = (Integer) properties.get("sizeOfSelectedColoumns");

           for (long i = 1; i <= size; i++)
               columnsStuff.add(properties.get(val + i));

           return columnsStuff.toArray(new Object[0]);
    }

    private Table dealWithTheHash(HashMap<String, Object> properties) throws SQLException {
        if (properties == null) throw new SQLException("OPS");

        Table selectedTable = getTheDesiredTable((String) properties.get("tableName"));
        if (selectedTable == null) throw new SQLException("NO TABLE EXIST");

        return selectedTable;
    }

    private String[] toStringArray(Object[] values)
    {
        ArrayList<String> stringValues = new ArrayList<>();

        for (Object val : values)
        {
            stringValues.add((String) val);
        }

        return stringValues.toArray(new String[0]);
    }
}
