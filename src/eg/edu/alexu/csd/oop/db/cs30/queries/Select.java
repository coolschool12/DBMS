package eg.edu.alexu.csd.oop.db.cs30.queries;

import eg.edu.alexu.csd.oop.db.Database;
import eg.edu.alexu.csd.oop.db.cs30.DataBaseGenerator;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * SELECT query, id: 7
 */
public class Select implements Query {
    private List<String> orderColumns = null;
    private List<Integer> whichOrder = null; // 0: ASC, 1: DESC

    @Override
    public boolean isCorrect(String query) {
        Pattern pattern = Pattern.compile("(^\\s*SELECT\\s+(([^\\s]+\\s*,\\s*)*\\s*([^\\s]+)|\\*)\\s+FROM\\s+[^\\s]+((\\s+WHERE\\s+.+)|\\s*)(\\s*;\\s*|\\s*)$)", Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();
    }

    @Override
    public int getId() {
        return 7;
    }

    @Override
    public void execute(Database database, String query) throws SQLException {
        Object[][] objects = setOrder(database.executeQuery(checkOrderBy(query)));
        print(DataBaseGenerator.getSelectedColumnNames());
        for (Object[] objectsArr : objects)
            print(objectsArr);
    }

    @Override
    public void execute(Statement statement, String query) throws SQLException {
        ResultSet resultSet = statement.executeQuery(query);

        ResultSetMetaData metaData = resultSet.getMetaData();

        for (int i = 0; i < metaData.getColumnCount(); i++)
            System.out.print(metaData.getColumnName(i) + "\t");

        System.out.println();

        while (resultSet.next()) {
            for (int i = 0; i < metaData.getColumnCount(); i++) {
                if (metaData.getColumnType(i) == 1) {
                    try{
                        System.out.print(resultSet.getInt(i) + "\t");
                    }catch ( NullPointerException e){
                        System.out.print("null" + "\t");
                    }
                } else {
                    try{
                        System.out.print(resultSet.getString(i) + "\t");
                    }catch ( NullPointerException e){
                        System.out.print("null" + "\t");
                    }
                }
            }
            System.out.println();
        }

    }

    @Override
    public boolean executeWithoutPrinting(Database database, String query) throws SQLException {
        return database.executeQuery(query).length > 0;
    }

    private Object[][] setOrder(Object[][]objects) {

        try{
            if (orderColumns != null)
            {
                Arrays.sort(objects, (o1, o2) -> {
                    for (int i = 0; i < orderColumns.size(); i++) {

                        String order = orderColumns.get(i);

                        int col = searchForCol(order, DataBaseGenerator.getSelectedColumnNames());

                        if (col == -1) throw new RuntimeException("THERE'S NO COLUMN WITH THAT NAME !!!!!!!!!!!!!");

                        if (whichOrder.get(i) == 1) {
                            if ((o1[col].toString()).compareTo(o2[col].toString()) > 0)
                                return 1;

                            else if ((o1[col].toString()).compareTo(o2[col].toString()) < 0)
                                return -1;
                        }
                        else
                        {
                            if ((o1[col].toString()).compareTo(o2[col].toString()) > 0)
                                return -1;

                            else if ((o1[col].toString()).compareTo(o2[col].toString()) < 0)
                                return 1;
                        }
                    }
                    return 0;
                });
            }
        } catch (RuntimeException e) {
            return objects;
        }
        return objects;
    }

    /**
     * Check if select contains order by and find columns to order by.
     */
    private String checkOrderBy(String query) throws SQLException {
        Pattern orderPattern = Pattern.compile("(^\\s*SELECT\\s+(([^\\s]+\\s*,\\s*)*\\s*([^\\s]+)|\\*)\\s+FROM\\s+[^\\s]+((\\s+WHERE\\s+.+)|\\s*)\\s*(ORDER\\s*BY(\\s*[^\\s]+\\s*(ASC|DESC|\\s*)\\s*,\\s*)*\\s*[^\\s]+\\s*(ASC|DESC|\\s*)\\s*)(\\s*;\\s*|\\s*)$)", Pattern.CASE_INSENSITIVE);
        Pattern splitPattern = Pattern.compile("(^\\s*SELECT\\s+(([^\\s]+\\s*,\\s*)*\\s*([^\\s]+)|\\*)\\s+FROM\\s+[^\\s]+((\\s+WHERE\\s+.+)|\\s*)\\s*ORDER\\s*BY\\s*|(\\s*;\\s*|\\s*)$)", Pattern.CASE_INSENSITIVE);

        // Contains ORDER BY
        if (orderPattern.matcher(query).matches())
        {
            String[] columns = ExtractData.removeEmptyStrings(splitPattern.split(query));

            if (columns.length == 1)
            {
                String[] splitOrderColumns = columns[0].split("(\\s*,\\s*)");

                // Fill order columns list
                orderColumns = new ArrayList<>();
                whichOrder = new ArrayList<>();

                for (String orderColumn : splitOrderColumns)
                {
                    String[] columnOrderSplit = ExtractData.removeEmptyStrings(orderColumn.split("\\s+"));

                    if (columnOrderSplit.length == 1)
                    {
                        orderColumns.add(columnOrderSplit[0]);
                        whichOrder.add(0);
                    }
                    else if (columnOrderSplit.length == 2)
                    {
                        orderColumns.add(columnOrderSplit[0]);

                        if (columnOrderSplit[1].equalsIgnoreCase("ASC"))
                        {
                            whichOrder.add(0);
                        }
                        else if (columnOrderSplit[1].equalsIgnoreCase("DESC"))
                        {
                            whichOrder.add(1);
                        }
                        else
                        {
                            throw new SQLException("Not ASC or DESC in ORDER BY");
                        }
                    }
                    else
                    {
                        throw new SQLException("Incorrect columns in ORDER BY");
                    }
                }
            }
            else
            {
                throw new SQLException("Incorrect ORDER BY");
            }

            return Pattern.compile("(\\s*ORDER\\s*BY(\\s*[^\\s]+\\s*(ASC|DESC|\\s*)\\s*,\\s*)*\\s*[^\\s]+\\s*(ASC|DESC|\\s*)\\s*)", Pattern.CASE_INSENSITIVE).matcher(query).replaceAll("");
        }
        else
        {
            return query;
        }
    }

    private int searchForCol(String col , Object[] colNames)
    {
        for (int i = 0; i < colNames.length; i++)
            if (col.equalsIgnoreCase((String) colNames[i])) return i;

        return -1;
    }

    private void print(Object[] row)
    {
        for (Object o : row) {
            System.out.print(o + "\t");
        }

        System.out.println();
    }
}
