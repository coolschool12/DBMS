package eg.edu.alexu.csd.oop.db.cs30;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Check if a query is valid and return extracted data
 */
public class DataChecker {
    /**
     * Check if CREATE DATABASE query is valid
     */
    boolean checkCreateDatabase(String query) {
        Pattern pattern = Pattern.compile("(^\\S*CREATE\\s*DATABASE\\s*([^\\s]*|[^\\s]*\\s*;\\s*$))", Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();
    }

    /**
     * Check if CREATE TABLE query is valid
     */
    boolean checkCreateTable(String query) {
        Pattern pattern = Pattern.compile("(^\\s*CREATE\\s*TABLE\\s*[^\\s]+\\s*\\((\\s*[^\\s]+\\s*(int|varchar)\\s*,)*\\s*[^\\s]+\\s*(int|varchar)\\s*\\)(\\s*;\\s*|\\s*)$)", Pattern.CASE_INSENSITIVE);
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
        String[] tableContent =  this.removeEmptyStrings(pattern.split(query));

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
     * Check DROP DATABASE query.
     */
    boolean checkDropDatabase(String query) {
        Pattern pattern = Pattern.compile("(^\\s*DROP\\s*DATABASE\\s*[^\\s*]+\\s*(;|\\s*)\\s*$)", Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();
    }

    /**
     * Check DROP TABLE query.
     */
    boolean checkDropTable(String query) {
        Pattern pattern = Pattern.compile("(^\\s*DROP\\s*TABLE\\s*[^\\s*]+\\s*(;|\\s*)\\s*$)", Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();
    }

    /**
     * Check if INSERT query is valid
     */
    boolean checkInsert(String query) {
        Pattern pattern = Pattern.compile("(^\\s*INSERT\\s*INTO\\s*[^\\s]+\\s*((\\((\\s*[^\\s]+\\s*,\\s*)*\\s*[^\\s]+\\s*\\))|(\\s*))\\s*VALUES\\s*\\((\\s*[^\\s]+\\s*,\\s*)*\\s*[^\\s]+\\s*\\)(\\s*|\\s*;\\s*)$)", Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();
    }

    /**
     * Get objects to be inserted from INSERT query.
     *
     * Column 0: values to insert
     * Column 1: names of the columns, can be null if no names are specified
     */
    Object[][] getObjectsToInsert(String query) {
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
        }

        objects[0] = values;

        return objects;
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
