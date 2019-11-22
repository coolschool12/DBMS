package eg.edu.alexu.csd.oop.db.cs30.queries;

import java.util.regex.Pattern;

/**
 * UPDATE query, id: 6
 */
public class Update implements Query {
    @Override
    public boolean isCorrect(String query) {
        Pattern pattern = Pattern.compile("(^\\s*UPDATE\\s+[^\\s]+\\s+SET\\s+([^\\s]+\\s*=\\s*[^\\s]+\\s*,\\s*)*([^\\s]+\\s*=\\s*[^\\s]+)(\\s+WHERE\\s+[^\\s]+\\s*[>=<]\\s*[^\\s]+|\\s*)(\\s*;\\s*|\\s*)$)",Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();
    }

    @Override
    public int getId() {
        return 6;
    }
}
