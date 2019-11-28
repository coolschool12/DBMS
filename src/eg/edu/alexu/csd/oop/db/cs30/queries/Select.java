package eg.edu.alexu.csd.oop.db.cs30.queries;

import eg.edu.alexu.csd.oop.db.Database;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * SELECT query, id: 7
 */
public class Select implements Query {
    private List<String> orders;
    private List<Integer> whichOrder; // 0: ASC, 1: DESC

    @Override
    public boolean isCorrect(String query) {
        Pattern pattern = Pattern.compile("(^\\s*SELECT\\s+(([^\\s]+\\s*,\\s*)*\\s*([^\\s]+)|\\*)\\s+FROM\\s+[^\\s]+((\\s+WHERE\\s+.+)|\\s*)(\\s*;\\s*|\\s*)$)", Pattern.CASE_INSENSITIVE);
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

    private String checkOrderBy(String query) throws SQLException {
        Pattern pattern = Pattern.compile("(^\\s*SELECT\\s+(([^\\s]+\\s*,\\s*)*\\s*([^\\s]+)|\\*)\\s+FROM\\s+[^\\s]+((\\s+WHERE\\s+.+)|\\s*)(\\s+ORDER\\s+BY\\s+(\\s*[^\\s]+\\s*(ASC|DESC)\\s*,)*\\s*(\\s*[^\\s]+\\s*(ASC|DESC)\\s*))(\\s*;\\s*|\\s*)$)", Pattern.CASE_INSENSITIVE);
        Pattern splitPattern = Pattern.compile("(^\\s*SELECT\\s+(([^\\s]+\\s*,\\s*)*\\s*([^\\s]+)|\\*)\\s+FROM\\s+[^\\s]+((\\s+WHERE\\s+.+)|\\s*)((\\s+ORDER\\s+BY\\s+)|\\s*)|(\\s*;\\s*|\\s*)$)", Pattern.CASE_INSENSITIVE);

        if (pattern.matcher(query).matches())
        {
            String[] splitStrings = splitPattern.split(query);
            if (splitStrings.length == 1)
            {
                String[] splitOrders = splitStrings[0].split("(\\s*,\\s*|\\s*)");

                orders = new ArrayList<>();
                whichOrder = new ArrayList<>();

                for  (int i = 0, length = splitOrders.length; i < length; i += 2)
                {
                    orders.add(splitOrders[i]);

                    if (splitOrders[i + 1].equalsIgnoreCase("ASC"))
                    {
                        whichOrder.add(0);
                    }
                    else if (splitOrders[i + 1].equalsIgnoreCase("DESC"))
                    {
                        whichOrder.add(1);
                    }
                    else
                    {
                        whichOrder.add(0);
                        i--;
                    }
                }
            }
            else
            {
                throw new SQLException();
            }

            return query.replaceAll("(\\s+ORDER\\s+BY\\s+(\\s*[^\\s]+\\s*(ASC|DESC)\\s*,)*\\s*(\\s*[^\\s]+\\s*(ASC|DESC)\\s*))", "");
        }
        else
        {
            return query;
        }
    }
}
