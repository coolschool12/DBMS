package eg.edu.alexu.csd.oop.db.cs30.queries;

import java.util.regex.Pattern;

/**
 * DROP DATABASE query, id: 2
 */
public class DropDatabase implements Query {
    @Override
    public boolean isCorrect(String query) {
        Pattern pattern = Pattern.compile("(^\\s*DROP\\s+DATABASE\\s+[^\\s*]+\\s*(;|\\s*)\\s*$)", Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();
    }

    @Override
    public int getId() {
        return 2;
    }
}
