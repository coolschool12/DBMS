package eg.edu.alexu.csd.oop.db.cs30;

import java.util.ArrayList;

public class MyDataBase {
    private ArrayList<Table>tables;
    private String name;
    private String path;

    MyDataBase(String name)
    {
        this.name = name;
        tables = new ArrayList<>();
    }

    public boolean addTable(Object[][] Data, String tableName)
    {
        ArrayList<String> ColumnsNames = new ArrayList<>();
        ArrayList<Integer> ColumnsTypes = new ArrayList<>();

        for (int i = 0; i < 2 ; i++)
            for (int j = 0; j < Data[i].length; j++) {
                ColumnsNames.add((String) Data[i][j]);
                ColumnsTypes.add(i);
            }

        Table newTable = new Table(ColumnsNames.toArray(new String[0]), ColumnsTypes.toArray(new Integer[0]));
        newTable.setTableName(tableName);
        tables.add(newTable);

        return true;

    }

    public boolean removeTable(String tableName)
    {
        Table desiredTable = getTheDesiredTable(tableName);

        return desiredTable != null;

    }

    public int editTable(Object[][] newContent, String tableName)
    {
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

    public String getName() {
        return name;
    }

    public String getPath() {
        return path;
    }
    private Table getTheDesiredTable(String tableName)
    {
        for (Table table : tables)
        {
            if (table.getTableName().equals(tableName))
            {
                return table;
            }
        }
        return null;
    }
}
