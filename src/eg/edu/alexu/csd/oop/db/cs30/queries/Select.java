package eg.edu.alexu.csd.oop.db.cs30.queries;

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
}
