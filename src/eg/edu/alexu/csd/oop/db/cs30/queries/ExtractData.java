package eg.edu.alexu.csd.oop.db.cs30.queries;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Extract useful data from a string and other helpful functions
 */
public class ExtractData {
    /**
     * Get names of columns and values to insert in a table from a CREATE query
     *
     * Column 0: int objects
     * Column 1: varchar objects
     */
    public Object[][] getContentsOfTableQuery(String query) {
        Pattern pattern = Pattern.compile("(\\s*CREATE\\s*TABLE\\s*[^\\s(]+\\s*|\\s*\\(\\s*|\\s*\\)\\s*;\\s*$|\\s*\\)\\s*$|\\s*,\\s*)", Pattern.CASE_INSENSITIVE);
        String[] tableContent =  this.removeEmptyStrings(pattern.split(query));

        // Extract column names from table
        List<String> ints = new ArrayList<>();
        List<String> varchars = new ArrayList<>();
        for (String nameTypeString : tableContent)
        {
            String[] splitNameTypeString = nameTypeString.split("\\s+");

            if (splitNameTypeString[1].equalsIgnoreCase("int"))
            {
                ints.add(splitNameTypeString[0].toLowerCase());
            }
            else if (splitNameTypeString[1].equalsIgnoreCase("varchar"))
            {
                varchars.add(splitNameTypeString[0].toLowerCase());
            }
        }

        String[][] tableColumns = new String[2][];
        tableColumns[0] = ints.toArray(new String[0]);
        tableColumns[1] = varchars.toArray(new String[0]);

        return tableColumns;
    }

    /**
     * Get objects to be inserted from INSERT query.
     *
     * Column 0: values to insert
     * Column 1: names of the columns, can be null if no names are specified
     */
    public Object[][] getObjectsToInsert(String query) {
        Pattern pattern = Pattern.compile("(^\\s*INSERT\\s*INTO\\s*[^\\s^(]+\\s*|\\s*VALUES\\s*|\\s*;\\s*$)", Pattern.CASE_INSENSITIVE);
        String[] twoLists = this.removeEmptyStrings(pattern.split(query));

        Object[][] objects = new Object[2][];

        // Without column names
        if (twoLists.length == 1)
        {
            objects[0] = this.removeEmptyStrings(twoLists[0].split("(\\s*,\\s*|\\s+|\\(|\\))"));
        }
        // With column names
        else if (twoLists.length == 2)
        {
            objects[0] = this.removeEmptyStrings(twoLists[1].split("(\\s*,\\s*|\\s+|\\(|\\))"));
            objects[1] = this.removeEmptyStrings(twoLists[0].split("(\\s*,\\s*|\\s+|\\(|\\))"));

            if (objects[0].length != objects[1].length)
            {
                return null;
            }
        }
        else
        {
            return null;
        }

        // Cast value strings
        Object[] values = new Object[objects[0].length];

        for (int i = 0, noOfObjects = objects[0].length; i < noOfObjects; i++)
        {
            // String input
            if (Pattern.matches("(^'.*'$)", (String) objects[0][i]))
            {
                values[i] = ((String) objects[0][i]).replaceAll("(^'|'$)", "");
            }
            // Integer input
            else
            {
                values[i] = Integer.parseInt((String) objects[0][i]);
            }

            // Cast column names to lowercase
            objects[1][i] = ((String) objects[1][i]).toLowerCase();
        }

        objects[0] = values;

        return objects;
    }

    /**
     * the map contians every thing like tablename selectedcoloumns etc...
     * @param
     * @return
     */
    public Map<String , String> SelectedProperties(String s) {
        s = s.replaceAll("'","");
        s = s.toLowerCase();
        String condValue;
        Map <String , String> table = new HashMap<>();
        table.put("type","0");
        table.put("starflag" , "0");

        if(s.toLowerCase().contains("*") && s.toLowerCase().contains("where")){
            table.put("starflag" , "1");
            String[] splitQuery = s.split("(FROM|from)\\s+|(WHERE|where)\\s+|(=|<|>)\\s*|'");
            if(s.toLowerCase().contains("=")) table.put("operator" , "=");
            else if(s.toLowerCase().contains(">")) table.put("operator" , ">");
            else if(s.toLowerCase().contains("<")) table.put("operator" , "<");

            table.put("tableName" , splitQuery[1]);
            table.put("condColumns" , splitQuery[2]);
            table.put("condValue" , splitQuery[3]);

            condValue= splitQuery[3];
            if(condValue.matches("\\d+"))
                table.put("type","1");
        }
        else if(s.toLowerCase().contains("*")){
            table.put("starflag" , "1");
            String[] splitQuery = s.split("(from|FROM)\\s*");
            table.put("tableName" , splitQuery[1]);
        }else if(s.toLowerCase().contains("where")){
            String[] splitQuery = s.split("[\\s,]+");

            int i;
            for( i=1;!splitQuery[i].equalsIgnoreCase("from");i++){
                table.put("selectedColumn"+ i,splitQuery[i]);
            }
            table.put("sizeOfSelectedColoumns",Integer.toString(i-1));

            splitQuery = s.split("(FROM|from)\\s+|(WHERE|where)\\s+|(=|<|>)\\s*|'");
            if(s.toLowerCase().contains("=")) table.put("operator" , "=");
            else if(s.toLowerCase().contains(">")) table.put("operator" , ">");
            else if(s.toLowerCase().contains("<")) table.put("operator" , "<");

            table.put("tableName" , splitQuery[1]);
            table.put("condColumns" , splitQuery[2]);
            table.put("condValue" , splitQuery[3]);
            condValue = splitQuery[3];
            if(condValue.matches("\\d+"))
                table.put("type","1");
        }
        else{
            String[] splitQuery = s.split("[\\s,]+");
            int i;
            for( i=1;!splitQuery[i].equalsIgnoreCase("from");i++){
                table.put("selectedColumn"+ i,splitQuery[i]);
            }
            table.put("sizeOfSelectedColoumns",Integer.toString(i-1));
            table.put("tableName" , splitQuery[i+1]);
        }
        return table;

    }

    public Map<String , String> DeleteProperties(String s){
        s = s.replaceAll("'","");

        s = s.toLowerCase();
        Map <String , String> table = new HashMap<>();
        table.put("type","0");
        if(s.toLowerCase().contains("where")){
            Pattern pattern = Pattern.compile("(FROM|from)\\s*|\\s*(WHERE|where)\\s*|(=|<|>)\\s*|'", Pattern.CASE_INSENSITIVE);
            String[] splitQuery = pattern.split(s);
            if(s.toLowerCase().contains("=")) table.put("operator" , "=");
            else if(s.toLowerCase().contains(">")) table.put("operator" , ">");
            else if(s.toLowerCase().contains("<")) table.put("operator" , "<");

            table.put("tableName" , splitQuery[1]);
            table.put("condColumns" , splitQuery[2]);
            table.put("condValue" , splitQuery[3]);
            if(splitQuery[3].matches("\\d+"))
                table.put("type","1");

        }else {
            String[] splitQuery = s.split("\\s+");
            table.put("tableName" , splitQuery[2]);
        }
        return table;
    }

    public Map<String , String> UpadteProperties( String s){
        s = s.replaceAll("'","");
        s = s.toLowerCase();
        Map <String , String> table = new HashMap<>();

        if(s.toLowerCase().contains("where")){
            table.put("type","0");
            String[] splitQuery = s.split("(UPDATE|update)\\s|(SET|set)\\s+|(WHERE|where)\\s+|(=|<|>)\\s*|'|\\s*,\\s*");


            if(s.toLowerCase().contains(">")) table.put("operator" , ">");
            else if(s.toLowerCase().contains("<")) table.put("operator" , "<");
            else if(s.toLowerCase().contains("=")) table.put("operator" , "=");

            int m =1;
            for(int j=0;j<(splitQuery.length-4);){
                table.put("selectedColumn"+ m,splitQuery[j+2]);
                table.put("setValue"+ m++,splitQuery[j+3]);
                j+=2;
            }
            table.put("sizeOfSelectedColoumns" ,Integer.toString(m-1));
            table.put("tableName" , splitQuery[1]);
            table.put("condColumns" , splitQuery[splitQuery.length-2]);
            table.put("condValue" , splitQuery[splitQuery.length-1]);
            if(splitQuery[splitQuery.length-1].matches("\\d+"))
                table.put("type","1");

        }else {
            String[] splitQuery = s.split("(UPDATE|update)\\s|\\s*=\\s*|(SET|set)\\s+|'|\\s*,\\s*");
            int m =1;
            for(int j=0;j<(splitQuery.length-2);){
                table.put("selectedColumn"+ m,splitQuery[j+2]);
                table.put("setValue"+ m++,splitQuery[j+3]);
                j+=2;
            }
            table.put("sizeOfSelectedColoumns" ,Integer.toString(m-1));
            table.put("tableName" , splitQuery[1]);
        }
        return table;
    }

    /**
     * @return table's name extracted from a query
     */
    public String getTableName(String query) {
        Pattern pattern = Pattern.compile("(\\s*((CREATE|DROP)\\s*TABLE|INSERT\\s*INTO|UPDATE|DELETE\\s*FROM|SELECT.*FROM)\\s*|\\s*VALUES\\s*|\\s*SET.*|\\s*WHERE.*|\\s*\\(.*\\)\\s*|\\s*;\\s*$)", Pattern.CASE_INSENSITIVE);
        String[] splitQuery = this.removeEmptyStrings(pattern.split(query));

        return splitQuery[0].toLowerCase();
    }

    /**
     * @return database's name extracted from a query
     */
    public String getDatabaseName(String query) {
        Pattern pattern = Pattern.compile("(^\\s*(CREATE|DROP)\\s*DATABASE\\s*|\\s*;\\s*$)", Pattern.CASE_INSENSITIVE);
        String[] splitQuery = this.removeEmptyStrings(pattern.split(query));

        return splitQuery[0].toLowerCase();
    }

    /**
     * Remove empty strings from an array
     */
    private String[] removeEmptyStrings(String[] stringsArray) {
        List<String> removeEmptyElements = new ArrayList<>(Arrays.asList(stringsArray));
        removeEmptyElements.removeAll(Collections.singleton(""));

        return removeEmptyElements.toArray(new String[0]);
    }


  

}
