package eg.edu.alexu.csd.oop.db.cs30.queries;

import eg.edu.alexu.csd.oop.db.Database;

import java.sql.SQLException;
import java.util.regex.Pattern;

/**
 * CREATE DATABASE query, id: 0
 */
public class CreateDatabase implements Query {
    @Override
    public boolean isCorrect(String query) {
        Pattern pattern = Pattern.compile("(^\\S*CREATE\\s+DATABASE\\s+([^\\s]*|[^\\s]*\\s*;\\s*$))", Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();
    }

    @Override
    public int getId() {
        return 0;
    }

    @Override
    public void execute(Database database, String query) throws SQLException {
        if(database.executeStructureQuery(query))
        {
            System.out.println("Database was created successfully.");
        }
        else
        {
            System.out.println("Error while creating database.");
        }
    }

    @Override
    public boolean executeWithoutPrinting(Database database, String query) throws SQLException {
        return database.executeStructureQuery(query);
    }
}
