package eg.edu.alexu.csd.oop.db.cs30;

import eg.edu.alexu.csd.oop.db.Database;
import eg.edu.alexu.csd.oop.db.cs30.queries.Query;
import eg.edu.alexu.csd.oop.db.cs30.queries.QueryBuilder;

import java.util.Scanner;

public class UI {
    public static void main(String[] args) {
        Database database = DataBaseGenerator.makeInstance();

        // Take user input
        String queryString = "";
        Scanner sc = new Scanner(System.in);

        // Create and evaluate a query
        while (true)
        {
            System.out.print("Insert a query (or q to quit): ");

            try {
                if (sc.hasNextLine())
                {
                    queryString = sc.nextLine();
                }

                if (queryString.equalsIgnoreCase("q"))
                {
                    break;
                }
                System.out.println();

                Query query = QueryBuilder.buildQuery(queryString.toLowerCase());
                query.execute(database, queryString.toLowerCase());
            }
            catch (Exception e) {
                System.out.println(e.getMessage());
            }

            System.out.println();
        }
    }
}
