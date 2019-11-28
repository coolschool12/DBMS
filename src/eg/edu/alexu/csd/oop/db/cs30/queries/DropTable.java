package eg.edu.alexu.csd.oop.db.cs30.queries;

import eg.edu.alexu.csd.oop.db.Database;

import java.sql.SQLException;
import java.util.regex.Pattern;

/**
 * DROP TABLE query, id: 3
 */
public class DropTable implements Query {
    @Override
    public boolean isCorrect(String query) {
        Pattern pattern = Pattern.compile("(^\\s*DROP\\s+TABLE\\s+[^\\s*]+\\s*(;|\\s*)\\s*$)", Pattern.CASE_INSENSITIVE);
        return pattern.matcher(query).matches();
    }

    @Override
    public int getId() {
        return 3;
    }

    @Override
    public void execute(Database database, String query) throws SQLException {
        if(database.executeStructureQuery(query))
        {
            System.out.println("Table was dropped successfully.");
        }
        else
        {
            System.out.println("Error while dropping table.");
        }
    }
}
