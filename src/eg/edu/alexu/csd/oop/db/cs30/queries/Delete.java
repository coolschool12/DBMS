package eg.edu.alexu.csd.oop.db.cs30.queries;

import java.util.regex.Pattern;

/**
 * DELETE query, id: 5
 */
public class Delete implements Query {
    @Override
    public boolean isCorrect(String query) {
        Pattern pattern = Pattern.compile("(^\\s*DELETE\\s+FROM\\s+[^\\s]+(\\s+WHERE\\s+[^\\s]+\\s+[><=]\\s+[^\\s]|\\s*)(\\s*;\\s*|\\s*)$)", Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();
    }

    @Override
    public int getId() {
        return 5;
    }
}
