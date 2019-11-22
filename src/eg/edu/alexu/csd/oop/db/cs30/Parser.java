package eg.edu.alexu.csd.oop.db.cs30;

import eg.edu.alexu.csd.oop.db.Database;

import java.sql.SQLException;
import java.util.regex.Pattern;

public class Parser {
    /**
     * Check sql query and call appropriate function.
     */
    public boolean checkSql(String query) {
        Database database = new DataBaseGenerator();
        String[] splitQuery = query.split(" ", 3);

        // Check number of substrings
        if (splitQuery.length != 3)
        {
            return false;
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
                    return false;
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
                    return false;
                }
            }
            else
            {
                // Syntax error
                return false;
            }
        }
        else if (splitQuery[0].equalsIgnoreCase("insert") || splitQuery[0].equalsIgnoreCase("update") || splitQuery[0].equalsIgnoreCase("delete"))
        {
            try {
                database.executeUpdateQuery(query);
            }
            catch (SQLException e) {
                // Syntax error
                return false;
            }
        }
        else if (splitQuery[0].equalsIgnoreCase("select"))
        {
            try {
                database.executeQuery(query);
            }
            catch (SQLException e) {
                // Syntax error
                return false;
            }
        }
        else if (splitQuery[0].equalsIgnoreCase("drop"))
        {
            try {
                database.executeStructureQuery(query);
            }
            catch (SQLException e) {
                // Syntax error
                return false;
            }
        }
        else
        {
            // Syntax error
            return false;
        }

        return true;
    }
}
