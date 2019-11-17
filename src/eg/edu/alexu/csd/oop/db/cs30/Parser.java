package eg.edu.alexu.csd.oop.db.cs30;

public class Parser {
    boolean checkSql(String query) {
        String[] splitQuery = query.split(" ");

        if (splitQuery[0].equalsIgnoreCase("insert"))
        {

        }
        else if (splitQuery[0].equalsIgnoreCase("create"))
        {

        }
        else if (splitQuery[0].equalsIgnoreCase("drop"))
        {

        }
        else if (splitQuery[0].equalsIgnoreCase("update"))
        {

        }
        else if (splitQuery[0].equalsIgnoreCase("select"))
        {

        }
        else if (splitQuery[0].equalsIgnoreCase("delete"))
        {
        }
        else
        {
            return false;
        }
    }
}
