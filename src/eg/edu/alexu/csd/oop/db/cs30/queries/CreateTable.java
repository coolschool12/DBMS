package eg.edu.alexu.csd.oop.db.cs30.queries;

import java.util.regex.Pattern;

/**
 * CREATE TABLE query, id: 1
 */
public class CreateTable implements Query {
    @Override
    public boolean isCorrect(String query) {
        Pattern pattern = Pattern.compile("(^\\s*CREATE\\s+TABLE\\s+[^\\s]+\\s*\\((\\s*[^\\s]+\\s+(int|varchar)\\s*,)*\\s*[^\\s]+\\s+(int|varchar)\\s*\\)(\\s*;\\s*|\\s*)$)", Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();
    }

    @Override
    public int getId() {
        return 1;
    }
}
