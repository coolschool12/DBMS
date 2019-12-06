package eg.edu.alexu.csd.oop.db.cs30.queries;

import eg.edu.alexu.csd.oop.db.Database;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Pattern;

/**
 * DROP DATABASE query, id: 2
 */
public class DropDatabase implements Query {
    @Override
    public boolean isCorrect(String query) {
        Pattern pattern = Pattern.compile("(^\\s*DROP\\s+DATABASE\\s+[^\\s*]+\\s*(;|\\s*)\\s*$)", Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();
    }

    @Override
    public int getId() {
        return 2;
    }

    @Override
    public void execute(Database database, String query) throws SQLException {
        print(database.executeStructureQuery(query));
    }

    @Override
    public void execute(Statement statement, String query) throws SQLException {
        print(statement.execute(query));
    }

    @Override
    public boolean executeWithoutPrinting(Database database, String query) throws SQLException {
        return database.executeStructureQuery(query);
    }

    private void print(boolean isitCorrect)
    {
        if (isitCorrect)
        {
            System.out.println("Database was dropped successfully.");
        }
        else
        {
            System.out.println("Error while dropping database.");
        }

    }
}
