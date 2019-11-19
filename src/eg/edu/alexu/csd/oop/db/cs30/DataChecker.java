package eg.edu.alexu.csd.oop.db.cs30;

import java.awt.*;
import java.util.*;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Check if a query is valid and return extracted data
 */
public class DataChecker {
    boolean checkSelect(String query){
        Pattern pattern = Pattern.compile("(select|SELECT)\\s+([a-zA-Z0-9_]+\\s*,*\\s*)+\\s+(from|FROM)\\s[a-zA-Z0-9_]+\\s+(where|WHERE)\\s*[a-zA-Z0-9_]+\\s*(=|>|<)\\s*[a-zA-Z0-9'\\s_]+|(select|SELECT)\\s+([a-zA-Z0-9_]+\\s*,*\\s*)+\\s+(from|FROM)\\s[a-zA-Z0-9_]+|(select|SELECT)+\\s*\\*\\s+(from|FROM)\\s[a-zA-Z0-9_]+\\s*(where|WHERE)\\s*[a-zA-Z0-9_]+\\s*(=|>|<)\\s*[a-zA-Z0-9'\\s_]+|(select|SELECT)+\\s*\\*\\s+(from|FROM)\\s[a-zA-Z0-9_]+",Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();
    }

    /**
     * the map contians every thing like tablename selectedcoloumns etc...
     * @param s
     * @return
     */
    private Map<String , String> SelectedProperties(String s ) {
        s = s.replaceAll("'","");
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

            int i=1;
            for( i=1;!splitQuery[i].equalsIgnoreCase("from");i++){
                table.put("selectedColumn"+ Integer.toString(i) ,splitQuery[i]);
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
            int i=1;
            for( i=1;!splitQuery[i].equalsIgnoreCase("from");i++){
                table.put("selectedColumn"+ Integer.toString(i) ,splitQuery[i]);
            }
            table.put("sizeOfSelectedColoumns",Integer.toString(i-1));
            table.put("tableName" , splitQuery[i+1]);
        }
        return table;

    }

    boolean checkDelete(String query){
        Pattern pattern = Pattern.compile("(delete|DELETE)\\s+(FROM|from)\\s+[^\\s]+\\s*(WHERE|where)\\s*[a-zA-Z0-9_-]+\\s*(<|>|=)\\s*[a-zA-Z0-9'_\\s]+|(delete|DELETE)\\s+(FROM|from)\\s+[a-zA-Z0-9'_]+",Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();
    }
    private Map<String , String> DeleteProperties( String s){
        s = s.replaceAll("'","");
        Map <String , String> table = new HashMap<>();
        table.put("type","0");
        if(s.toLowerCase().contains("where")){
            String[] splitQuery = s.split("(FROM|from)\\s+|(WHERE|where)\\s+|(=|<|>)\\s*|'");
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

    boolean checkUpdate(String query){
        Pattern pattern = Pattern.compile("(UPDATE|update)\\s+[^\\s]+\\s+(SET|set)\\s+([a-zA-Z0-9_]+\\s*(=|<|>)\\s*[a-zA-Z0-9'_\\s]+,*\\s*)+(WHERE|where)\\s*[a-zA-Z0-09_]+\\s*(=|<|>)\\s*[a-zA-Z0-9_\\s]+|(UPDATE|update)\\s+[^\\s]+\\s+(SET|set)\\s+([a-zA-Z0-9_]+\\s*(=|<|>)\\s*[a-zA-Z0-9'_\\s]+,*\\s*)+",Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();
    }
    private Map<String , String> UpadteProperties( String s){
        s = s.replaceAll("'","");
        Map <String , String> table = new HashMap<>();

        if(s.toLowerCase().contains("where")){
            table.put("type","0");
            String[] splitQuery = s.split("(UPDATE|update)\\s|(SET|set)\\s+|(WHERE|where)\\s+|(=|<|>)\\s*|'|\\s*,\\s*");

            for(int i=0;i<splitQuery.length;i++){
                System.out.println(splitQuery[i]);
            }

            if(s.toLowerCase().contains(">")) table.put("operator" , ">");
            else if(s.toLowerCase().contains("<")) table.put("operator" , "<");
            else if(s.toLowerCase().contains("=")) table.put("operator" , "=");

            int m =1;
            for(int j=0;j<=(splitQuery.length-4)/2;j++){
                table.put("setColumn"+Integer.toString(m),splitQuery[j+2]);
                table.put("setValue"+Integer.toString(m++),splitQuery[j+3]);
                j++;
            }
            table.put("sizeofsetcolumns" ,Integer.toString(m-1));
            table.put("tableName" , splitQuery[1]);
            table.put("condColumns" , splitQuery[splitQuery.length-2]);
            table.put("condValue" , splitQuery[splitQuery.length-1]);
            if(splitQuery[splitQuery.length-1].matches("\\d+"))
                table.put("type","1");

        }else {
            String[] splitQuery = s.split("(UPDATE|update)\\s|\\s*=\\s*|(SET|set)\\s+|'|\\s*,\\s*");
            int m =1;
            for(int j=0;j<=(splitQuery.length-2)/2;j++){
                table.put("setColumn"+Integer.toString(m),splitQuery[j+2]);
                table.put("setValue"+Integer.toString(m++),splitQuery[j+3]);
                j++;
            }
            table.put("tableName" , splitQuery[1]);
        }
        return table;
    }

    /**
     * Check if CREATE query is valid
     */
    boolean checkCreate(String query) {
        Pattern pattern = Pattern.compile("(^\\S*CREATE\\s*DATABASE\\s*([^\\s]*|[^\\s]*\\s*;\\s*$))|(^\\s*CREATE\\s*TABLE\\s*[^\\s]+\\s*\\((\\s*[^\\s]+\\s*(int|varchar)\\s*,)*\\s*[^\\s]+\\s*(int|varchar)\\s*\\)(\\s*;\\s*|\\s*)$)", Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();
    }

    /**
     * Get names of columns and values to insert in a table from a CREATE query
     *
     * Column 0: int objects
     * Column 1: varchar objects
     */
    Object[][] getContentsOfTableQuery(String query) {
        Pattern pattern = Pattern.compile("(\\s*CREATE\\s*TABLE\\s*[^\\s(]+\\s*|\\s*\\(\\s*|\\s*\\)\\s*;\\s*$|\\s*\\)\\s*$|\\s*,\\s*)", Pattern.CASE_INSENSITIVE);
        String[] tableContent = pattern.split(query);

        // Remove empty strings from array
        List<String> removeEmptyElements = new ArrayList<>(Arrays.asList(tableContent));
        removeEmptyElements.removeAll(Collections.singleton(""));

        tableContent = removeEmptyElements.toArray(new String[0]);

        // Extract column names from table
        List<String> ints = new ArrayList<>();
        List<String> varchars = new ArrayList<>();
        for (String nameTypeString : tableContent)
        {
            String[] splitNameTypeString = nameTypeString.split("\\s+");

            if (splitNameTypeString[1].equalsIgnoreCase("int"))
            {
                ints.add(splitNameTypeString[0]);
            }
            else if (splitNameTypeString[1].equalsIgnoreCase("varchar"))
            {
                varchars.add(splitNameTypeString[0]);
            }
        }

        String[][] tableColumns = new String[2][];
        tableColumns[0] = ints.toArray(new String[0]);
        tableColumns[1] = varchars.toArray(new String[0]);

        return tableColumns;
    }

    /**
     * Check if INSERT statement is valid
     */
    boolean checkInsert(String query) {
        Pattern pattern = Pattern.compile("(^\\s*INSERT\\s*INTO\\s*[^\\s]+\\s*((\\((\\s*[^\\s]+\\s*,\\s*)*\\s*[^\\s]+\\s*\\))|(\\s*))\\s*VALUES\\s*\\((\\s*[^\\s]+\\s*,\\s*)*\\s*[^\\s]+\\s*\\)(\\s*|\\s*;\\s*)$)", Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();
    }

    /**
     * Get objects to be inserted from INSERT query.
     *
     * Column 0: values to insert
     * Column 1: names of the columns, can be null if no other values are specified
     */
    Object[][] getObjectsToInsert(String query) {
        Pattern pattern = Pattern.compile("(^\\s*INSERT\\s*INTO\\s*[^\\s^(]+\\s*|\\s*VALUES\\s*|\\s*;\\s*$)", Pattern.CASE_INSENSITIVE);
        String[] twoLists = pattern.split(query);

        Object[][] objects = new Object[2][];

        // Without column names
        if (twoLists.length == 1)
        {
            objects[0] = twoLists[0].split("(,|\\s*)");
        }
        // With column names
        else if (twoLists.length == 2)
        {
            objects[0] = twoLists[0].split("(,|\\s*)");
            objects[1] = twoLists[1].split("(,|\\s*)");
        }
        else
        {
            return null;
        }

        return objects;
    }
}
