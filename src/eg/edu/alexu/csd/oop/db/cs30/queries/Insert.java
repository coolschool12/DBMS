package eg.edu.alexu.csd.oop.db.cs30.queries;

import eg.edu.alexu.csd.oop.db.Database;

import java.sql.SQLException;
import java.util.regex.Pattern;

/**
 * INSERT INTO query, id: 4
 */
public class Insert implements Query {
    @Override
    public boolean isCorrect(String query) {
        Pattern pattern = Pattern.compile("(^\\s*INSERT\\s+INTO\\s+[^\\s]+\\s*((\\((\\s*[^\\s]+\\s*,\\s*)*\\s*[^\\s]+\\s*\\))|(\\s*))\\s*VALUES\\s*\\((\\s*[^\\s]+\\s*,\\s*)*\\s*[^\\s]+\\s*\\)(\\s*|\\s*;\\s*)$)", Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();
    }

    @Override
    public int getId() {
        return 4;
    }

    @Override
    public void execute(Database database, String query) throws SQLException {
        int numberOfRows = database.executeUpdateQuery(query);
        System.out.println(numberOfRows + " rows were changed.");
    }
}
