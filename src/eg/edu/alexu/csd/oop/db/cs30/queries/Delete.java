package eg.edu.alexu.csd.oop.db.cs30.queries;

import eg.edu.alexu.csd.oop.db.Database;

import java.sql.SQLException;
import java.util.regex.Pattern;

/**
 * DELETE query, id: 5
 */
public class Delete implements Query {

    @Override
    public boolean isCorrect(String query) {
        Pattern pattern = Pattern.compile("(^\\s*DELETE\\s+FROM\\s+[^\\s]+((\\s+WHERE\\s+.+)|\\s*)(\\s*;\\s*|\\s*)$)", Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();
    }

    @Override
    public int getId() {
        return 5;
    }

    @Override
    public void execute(Database database, String query) throws SQLException {
        int numberOfRows = database.executeUpdateQuery(query);
        System.out.println(numberOfRows + " rows were changed.");
    }

    @Override
    public boolean executeWithoutPrinting(Database database, String query) throws SQLException {
        return database.executeUpdateQuery(query) > 0;
    }
}
