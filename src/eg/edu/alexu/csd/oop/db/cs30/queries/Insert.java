package eg.edu.alexu.csd.oop.db.cs30.queries;

import eg.edu.alexu.csd.oop.db.Database;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Pattern;

/**
 * INSERT INTO query, id: 4
 */
public class Insert implements Query {
    @Override
    public boolean isCorrect(String query) {
        Pattern pattern = Pattern.compile("(^\\s*INSERT\\s+INTO\\s+[^\\s]+\\s*((\\((\\s*[^\\s]+\\s*,\\s*)*\\s*[^\\s]+\\s*\\))|(\\s*))\\s*VALUES\\s*\\((\\s*((('.*')|(\".*\"))|\\d+)+\\s*,\\s*)*\\s*((('.*')|(\".*\"))|\\d+)\\s*\\)(\\s*|\\s*;\\s*)$)", Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();
    }

    @Override
    public int getId() {
        return 4;
    }

    @Override
    public void execute(Database database, String query) throws SQLException {
        print(database.executeUpdateQuery(query));
    }

    @Override
    public void execute(Statement statement, String query) throws SQLException {
        print(statement.executeUpdate(query));
    }

    @Override
    public boolean executeWithoutPrinting(Database database, String query) throws SQLException {
        return database.executeUpdateQuery(query) > 0;
    }

    private void print(int numOfRows)
    {
        if (numOfRows == 0)
            System.out.println("Nothing Changed!");

        else
            System.out.println(numOfRows + " rows were changed.");
    }
}
