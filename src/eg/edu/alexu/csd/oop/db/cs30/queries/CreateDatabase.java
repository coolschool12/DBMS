package eg.edu.alexu.csd.oop.db.cs30.queries;

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
}
