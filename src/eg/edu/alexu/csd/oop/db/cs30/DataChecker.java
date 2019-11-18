package eg.edu.alexu.csd.oop.db.cs30;

import java.awt.*;
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
