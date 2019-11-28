package eg.edu.alexu.csd.oop.db.cs30.queries;

import eg.edu.alexu.csd.oop.db.Database;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * SELECT query, id: 7
 */
public class Select implements Query {
    @Override
    public boolean isCorrect(String query) {
        Pattern pattern = Pattern.compile("(^\\s*SELECT\\s+(([^\\s]+\\s*,\\s*)*\\s*([^\\s]+)|\\*)\\s+FROM\\s+[^\\s]+(\\s*|(\\s+WHERE\\s+[^\\s]+\\s+[><=]\\s+[^\\s]))(\\s*;\\s*|\\s*)$)", Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();
    }

    @Override
    public int getId() {
        return 7;
    }

    @Override
    public void execute(Database database, String query) throws SQLException {
        Object[][] objects = database.executeQuery(query);

        for (Object[] objectsArr : objects)
        {
            System.out.println(Arrays.toString(objectsArr));
        }
    }
}
