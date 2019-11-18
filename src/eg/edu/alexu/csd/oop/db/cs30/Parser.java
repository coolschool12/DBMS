package eg.edu.alexu.csd.oop.db.cs30;

import eg.edu.alexu.csd.oop.db.Database;

import java.sql.SQLException;
import java.util.regex.Pattern;

public class Parser {
    private Database database;

    /**
     * Check sql query and call appropriate function.
     */
    public void checkSql(String query) {
        String[] splitQuery = query.split(" ", 3);

        // Database = new DatabaseImplementation();

        // Check number of substrings
        if (splitQuery.length != 3)
        {
            return;
        }

        // Check statement type
        if (splitQuery[0].equalsIgnoreCase("create"))
        {
            // Create database
            if (splitQuery[1].equalsIgnoreCase("database"))
            {
                Pattern pattern = Pattern.compile("(\\s*CREATE\\s*DATABASE\\s*|\\s*;\\s*|\\s*$)", Pattern.CASE_INSENSITIVE);
                String[] databaseName = pattern.split(query);

                if (databaseName.length != 1)
                {
                    return;
                }

                database.createDatabase(databaseName[0], false);
            }
            // Create table
            else if (splitQuery[1].equalsIgnoreCase("table"))
            {
                try {
                    database.executeStructureQuery(query);
                }
                catch (SQLException e) {
                    // Syntax error
                }
            }
        }
        else if (splitQuery[0].equalsIgnoreCase("insert") || splitQuery[0].equalsIgnoreCase("update") || splitQuery[0].equalsIgnoreCase("delete"))
        {
            try {
                database.executeUpdateQuery(query);
            }
            catch (SQLException e) {
                // Syntax error
            }
        }
        else if (splitQuery[0].equalsIgnoreCase("select"))
        {
            try {
                database.executeQuery(query);
            }
            catch (SQLException e) {
                // Syntax error
            }
        }
        else if (splitQuery[0].equalsIgnoreCase("drop"))
        {
            try {
                database.executeStructureQuery(query);
            }
            catch (SQLException e) {
                // Syntax error
            }
        }
    }

    /**
     * Check drop statement.
     */
    private boolean checkDrop(String query) {
        // Split query into three parts
        String[] splitQuery = query.split(" ", 3);

        if (splitQuery.length == 3 && (splitQuery[1].equalsIgnoreCase("database") || splitQuery[1].equalsIgnoreCase("table")))
        {
            String[] databaseName = splitQuery[2].split(";|\\s");
            return databaseName.length == 1;
        }

        return false;
    }

    /**
     * Check create statement.
     */
    private boolean checkCreate(String query) {
        // Split query into three parts
        String[] splitQuery = query.split(" ", 3);

        if (splitQuery.length != 3)
        {
            return false;
        }

        // Create database
        if (splitQuery[1].equalsIgnoreCase("database"))
        {
            String[] databaseName = splitQuery[2].split(";|\\s");
            return databaseName.length == 1;
        }
        // Create table
        else if (splitQuery[1].equalsIgnoreCase("table"))
        {
            return this.checkElements(splitQuery[2]);
        }

        return false;
    }

    /**
     * Check insert statement.
     */
    private boolean checkInsert(String query) {
        String[] splitQuery = query.split(" ", 3);

        if (splitQuery.length != 3)
        {
            return false;
        }


        return false;
    }

    /**
     * Check if elements of a string are correctly specified.
     */
    private boolean checkElements(String elementsString) {
        String[] databaseInfo = elementsString.split("^\\s*\\(\\s*|\\s*,\\s*|\\s*\\)\\s*;\\s*$|\\s*\\)\\s*$");

        // Check every element type
        for (String element : databaseInfo)
        {
            String[] nameType = element.split(" ");

            if (nameType.length == 2 && (nameType[1].equalsIgnoreCase("int") || nameType[1].equalsIgnoreCase("varchar")))
            {
                return true;
            }
        }

        return false;
    }
}
