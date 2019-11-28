package eg.edu.alexu.csd.oop.db.cs30.queries;

import java.sql.SQLException;

/**
 * Builds a query object
 */
public class QueryBuilder {
    /**
     * @return a query object based on a string
     */
    public static Query buildQuery(String query) throws SQLException {
        String[] splitQuery = ExtractData.removeEmptyStrings(query.split(" "));

        // Check statement type
        if (splitQuery[0].equalsIgnoreCase("create"))
        {
            // Create database
            if (splitQuery[1].equalsIgnoreCase("database"))
            {
                return new CreateDatabase();
            }
            // Create table
            else if (splitQuery[1].equalsIgnoreCase("table"))
            {
                return new CreateTable();
            }
            else
            {
                throw new SQLException("Invalid query.");
            }
        }
        else if (splitQuery[0].equalsIgnoreCase("drop"))
        {
            // Create database
            if (splitQuery[1].equalsIgnoreCase("database"))
            {
                return new DropDatabase();
            }
            // Create table
            else if (splitQuery[1].equalsIgnoreCase("table"))
            {
                return new DropTable();
            }
            else
            {
                throw new SQLException("Invalid query.");
            }
        }
        else if (splitQuery[0].equalsIgnoreCase("insert"))
        {
            return new Insert();
        }
        else if (splitQuery[0].equalsIgnoreCase("update"))
        {
            return new Update();
        }
        else if (splitQuery[0].equalsIgnoreCase("delete"))
        {
            return new Delete();
        }
        else if (splitQuery[0].equalsIgnoreCase("select"))
        {
            return new Select();
        }
        else
        {
            throw new SQLException("Invalid query.");
        }
    }
}
