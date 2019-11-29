package eg.edu.alexu.csd.oop.db.cs30.queries;

import java.sql.SQLException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Extract useful data from a string and other helpful functions
 */
public class ExtractData {
    /**
     * @return names of columns and values to insert in a table from a CREATE query
     * Column 0: names of columns.
     * Column 1: contains 0 for string, 1 for an int.
     */
    private static ExtractData extractData = null;
    private ExtractData(){}

    public static ExtractData makeInstance()
    {
        if (extractData == null)
            extractData = new ExtractData();

        return extractData;
    }
    public Object[][] getContentsOfTableQuery(String query) {
        query = query.toLowerCase();

        Pattern pattern = Pattern.compile("(\\s*CREATE\\s*TABLE\\s*[^\\s(]+\\s*|\\s*\\(\\s*|\\s*\\)\\s*;\\s*$|\\s*\\)\\s*$|\\s*,\\s*)", Pattern.CASE_INSENSITIVE);
        String[] tableContent =  ExtractData.removeEmptyStrings(pattern.split(query));

        // Extract column names from table
        List<String> values = new ArrayList<>();
        List<Integer> whichValue = new ArrayList<>();
        for (String nameTypeString : tableContent)
        {
            String[] splitNameTypeString = nameTypeString.split("\\s+");

            values.add(splitNameTypeString[0].toLowerCase());
            if (splitNameTypeString[1].equalsIgnoreCase("int"))
            {
                whichValue.add(1);
            }
            else if (splitNameTypeString[1].equalsIgnoreCase("varchar"))
            {
                whichValue.add(0);
            }
        }

        Object[][] tableColumns = new Object[2][];
        tableColumns[0] = values.toArray(new String[0]);
        tableColumns[1] = whichValue.toArray(new Integer[0]);

        return tableColumns;
    }

    /**
     * Get objects to be inserted from INSERT query.
     *
     * @throws SQLException if query has incorrect values
     * @return an array of objects
     *      Column 0: values to insert
     *      Column 1: names of the columns, can be null if no names are specified
     */
    public Object[][] getObjectsToInsert(String query) throws SQLException {
        query = query.toLowerCase();

        Pattern pattern = Pattern.compile("(^\\s*INSERT\\s*INTO\\s*[^\\s^(]+\\s*|\\s*VALUES\\s*|\\s*;\\s*$)", Pattern.CASE_INSENSITIVE);
        String[] twoLists = ExtractData.removeEmptyStrings(pattern.split(query));

        Object[][] objects = new Object[2][];

        // Without column names
        if (twoLists.length == 1)
        {
            objects[0] = ExtractData.removeEmptyStrings(twoLists[0].split("((\\s*\\(\\s*)|(\\s*\\)\\s*)|(\\s*(?<=('|\"|\\d))\\s*,\\s*(?=('|\"|\\d))\\s*))"));
        }
        // With column names
        else if (twoLists.length == 2)
        {
            objects[0] = ExtractData.removeEmptyStrings(twoLists[1].split("((\\s*\\(\\s*)|(\\s*\\)\\s*)|(\\s*(?<=('|\"|\\d))\\s*,\\s*(?=('|\"|\\d))\\s*))"));
            objects[1] = ExtractData.removeEmptyStrings(twoLists[0].split("(\\s*,\\s*|\\s+|\\(|\\))"));

            if (objects[0].length != objects[1].length)
            {
                throw new SQLException("Non matching lengths.");
            }
        }
        else
        {
            throw new SQLException("No values given.");
        }

        // Cast value strings
        Object[] values = new Object[objects[0].length];

        for (int i = 0, noOfObjects = objects[0].length; i < noOfObjects; i++)
        {
            // String input
            if (Pattern.matches("(^('.*'|\".*\")$)", (String) objects[0][i]))
            {
                values[i] = ((String) objects[0][i]).replaceAll("(^'|'$)|(^\"|\"$)", "");
            }
            // Integer input
            else
            {
                try {
                    values[i] = Integer.parseInt((String) objects[0][i]);
                }
                catch (Exception e) {
                    throw new SQLException("Incorrect values.");
                }
            }
        }

        objects[0] = values;

        return objects;
    }

    /**
     * the map contians every thing like tablename selectedcoloumns etc...
     * @param
     * @return
     */
    public Map<String , Object> SelectedProperties(String s) {
        s = s.toLowerCase();
        Map <String , Object> table = new HashMap<>();

        table.put("starflag" , 0);

        if(s.toLowerCase().contains("*") && s.toLowerCase().contains("where")){
            table.put("starflag" , 1);
            String[] splitQuery = s.split("(FROM|from)\\s+|\\s*(WHERE|where)\\s+|\\s*(=|<|>)\\s*");

            table.put("tableName" , splitQuery[1]);
            splitQuery = s.split("\\s*(WHERE|where)\\s+");
            table.put("operator" , splitQuery[1]);

        }
        else if(s.toLowerCase().contains("*")){
            table.put("starflag" , 1);
            String[] splitQuery = s.split("(from|FROM)\\s*");
            table.put("tableName" , splitQuery[1]);
        }else if(s.toLowerCase().contains("where")){
            String[] splitQuery = s.split("\\s*select\\s*|\\s*,\\s*|\\s*from\\s*|\\s*where\\s*");

            int i;
            for( i=1;i<=splitQuery.length-3;i++){
                table.put("selectedColumn"+ i,splitQuery[i]);
            }
            table.put("sizeOfSelectedColoumns",splitQuery.length-3);
            table.put("tableName" , splitQuery[splitQuery.length-2]);
            splitQuery = s.split("\\s*(WHERE|where)\\s+");
            table.put("operator" , splitQuery[1]);
        }
        else{
            String[] splitQuery = s.split("[\\s,]+");
            int i;
            for( i=1;!splitQuery[i].equalsIgnoreCase("from");i++){
                table.put("selectedColumn"+ i,splitQuery[i]);
            }
            table.put("sizeOfSelectedColoumns",i-1);
            table.put("tableName" , splitQuery[i+1]);
        }
        return table;

    }

    public Map<String , Object> DeleteProperties(String s){

        s = s.toLowerCase();
        Map <String , Object> table = new HashMap<>();
        if(s.toLowerCase().contains("where")){
            Pattern pattern = Pattern.compile("(FROM|from)\\s*|\\s*(WHERE|where)\\s*|\\s*(=|<|>)\\s*", Pattern.CASE_INSENSITIVE);
            String[] splitQuery = pattern.split(s);


            table.put("tableName" , splitQuery[1]);

            splitQuery = s.split("\\s*(WHERE|where)\\s+");
            table.put("operator" , splitQuery[1]);

        }else {
            String[] splitQuery = s.split("\\s+");
            table.put("tableName" , splitQuery[2]);
        }
        return table;
    }

    public Map<String , Object> UpadteProperties(String s){
        s = s.toLowerCase();
        Map <String , Object> table = new HashMap<>();
        if(s.toLowerCase().contains("where")){
            String[] splited  = s.split("\\s*(WHERE|where)\\s+");
            String[] splitQuery = splited[0].split("(UPDATE|update)\\s*|\\s*(SET|set)\\s+|\\s*(=|<|>)\\s*|\\s*,\\s*");

            int m =1;
            for(int j=0;j<(splitQuery.length-2);){
                table.put("selectedColumn"+ m,splitQuery[j+2].replaceAll("'",""));
                if (splitQuery[j+3].contains("'")){ table.put("setValue"+ m++,splitQuery[j+3].replaceAll("'","")); }
                else { if(splitQuery[j+3].matches("\\d+")) table.put("setValue"+ m++,Integer.parseInt(splitQuery[j+3]));
                else return null; }
                j+=2;
            }

            table.put("sizeOfSelectedColoumns" ,m-1);
            table.put("tableName" , splitQuery[1]);
            splitQuery = s.split("\\s*(WHERE|where)\\s+");
            table.put("operator" , splitQuery[1]);
        }else {
            String[] splitQuery = s.split("(UPDATE|update)\\s*|\\s*=\\s*|\\s*(SET|set)\\s+|\\s*,\\s*");
            int m =1;
            for(int j=0;j<(splitQuery.length-2);){
                table.put("selectedColumn"+ m,splitQuery[j+2].replaceAll("'",""));
                if (splitQuery[j+3].contains("'")){ table.put("setValue"+ m++,splitQuery[j+3].replaceAll("'","")); }
                else { if(splitQuery[j+3].matches("\\d+")) table.put("setValue"+ m++,Integer.parseInt(splitQuery[j+3]));
                else return null; }

                j+=2;
            }
            table.put("sizeOfSelectedColoumns" ,m-1);
            table.put("tableName" , splitQuery[1]);
        }
        return table;
    }

    /**
     * @return table's name extracted from a query
     */
    public String getTableName(String query) {
        Pattern pattern = Pattern.compile("(\\s*((CREATE|DROP)\\s*TABLE|INSERT\\s*INTO|UPDATE|DELETE\\s*FROM|SELECT.*FROM)\\s*|\\s*VALUES\\s*|\\s*SET.*|\\s*WHERE.*|\\s*\\(.*\\)\\s*|\\s*;\\s*$)", Pattern.CASE_INSENSITIVE);
        String[] splitQuery = ExtractData.removeEmptyStrings(pattern.split(query));

        return splitQuery[0].toLowerCase();
    }

    /**
     * @return database's name extracted from a query
     */
    public String getDatabaseName(String query) {
        Pattern pattern = Pattern.compile("(^\\s*(CREATE|DROP)\\s*DATABASE\\s*|\\s*;\\s*$)", Pattern.CASE_INSENSITIVE);
        String[] splitQuery = ExtractData.removeEmptyStrings(pattern.split(query));

        return splitQuery[0].toLowerCase();
    }

    /**
     * Remove empty strings from an array
     */
    public static String[] removeEmptyStrings(String[] stringsArray) {
        List<String> removeEmptyElements = new ArrayList<>(Arrays.asList(stringsArray));
        removeEmptyElements.removeAll(Collections.singleton(""));

        return removeEmptyElements.toArray(new String[0]);
    }
}
