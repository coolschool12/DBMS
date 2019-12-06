package eg.edu.alexu.csd.oop.db.cs30.queries;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Builds a query object
 */
public class QueryBuilder {
    private static Map<ID, Query> queryMap = new HashMap<>();   // Map of objects for re usage

    enum ID {
        CREATE_DATABASE, CREATE_TABLE, DROP_DATABASE, DROP_TABLE, INSERT, UPDATE, DELETE, SELECT;
    }

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
                return QueryBuilder.getQuery(ID.CREATE_DATABASE);
            }
            // Create table
            else if (splitQuery[1].equalsIgnoreCase("table"))
            {
                return QueryBuilder.getQuery(ID.CREATE_TABLE);
            }
            else
            {
                throw new SQLException("Invalid query.");
            }
        }
        else if (splitQuery[0].equalsIgnoreCase("drop"))
        {
            // Drop database
            if (splitQuery[1].equalsIgnoreCase("database"))
            {
                return QueryBuilder.getQuery(ID.DROP_DATABASE);
            }
            // Drop table
            else if (splitQuery[1].equalsIgnoreCase("table"))
            {
                return QueryBuilder.getQuery(ID.DROP_TABLE);
            }
            else
            {
                throw new SQLException("Invalid query.");
            }
        }
        else if (splitQuery[0].equalsIgnoreCase("insert"))
        {
            return QueryBuilder.getQuery(ID.INSERT);
        }
        else if (splitQuery[0].equalsIgnoreCase("update"))
        {
            return QueryBuilder.getQuery(ID.UPDATE);
        }
        else if (splitQuery[0].equalsIgnoreCase("delete"))
        {
            return QueryBuilder.getQuery(ID.DELETE);
        }
        else if (splitQuery[0].equalsIgnoreCase("select"))
        {
            return QueryBuilder.getQuery(ID.SELECT);
        }
        else
        {
            throw new SQLException("Invalid query.");
        }
    }

    /**
     * @return an already initialized Query object
     */
    private static Query getQuery(ID id) {
        if (queryMap.get(id) == null)
        {
            Query whichQuery;

            switch (id)
            {
                case CREATE_DATABASE:
                    whichQuery = new CreateDatabase();
                    break;
                case CREATE_TABLE:
                    whichQuery = new CreateTable();
                    break;
                case DROP_DATABASE:
                    whichQuery = new DropDatabase();
                    break;
                case DROP_TABLE:
                    whichQuery = new DropTable();
                    break;
                case INSERT:
                    whichQuery = new Insert();
                    break;
                case SELECT:
                    whichQuery = new Select();
                    break;
                case UPDATE:
                    whichQuery = new Update();
                    break;
                case DELETE:
                default:
                    whichQuery = new Delete();
                    break;
            }

            queryMap.put(id, whichQuery);
        }

        return queryMap.get(id);
    }
}
