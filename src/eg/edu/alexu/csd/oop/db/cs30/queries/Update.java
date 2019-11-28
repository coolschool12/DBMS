package eg.edu.alexu.csd.oop.db.cs30.queries;

import eg.edu.alexu.csd.oop.db.Database;

import java.sql.SQLException;
import java.util.regex.Pattern;

/**
 * UPDATE query, id: 6
 */
public class Update implements Query {
    @Override
    public boolean isCorrect(String query) {
        Pattern pattern = Pattern.compile("(^\\s*UPDATE\\s+[^\\s]+\\s+SET\\s+([^\\s]+\\s*=\\s*(((\".*\")|('.*'))|\\d+)\\s*,\\s*)*([^\\s]+\\s*=\\s*(((\".*\")|('.*'))|\\d+))((\\s+WHERE\\s+.+)|\\s*)(\\s*;\\s*|\\s*)$)",Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();
    }

    @Override
    public int getId() {
        return 6;
    }

    @Override
    public void execute(Database database, String query) throws SQLException {
        int numberOfRows = database.executeUpdateQuery(query);
        System.out.println(numberOfRows + " rows were changed.");
    }
}
