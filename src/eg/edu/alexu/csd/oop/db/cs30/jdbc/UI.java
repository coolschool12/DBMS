package eg.edu.alexu.csd.oop.db.cs30.jdbc;

import eg.edu.alexu.csd.oop.db.cs30.DataBaseGenerator;
import eg.edu.alexu.csd.oop.db.cs30.queries.Query;
import eg.edu.alexu.csd.oop.db.cs30.queries.QueryBuilder;

import java.io.File;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Scanner;
import java.util.logging.Logger;

public class UI {

    public static void main(String[] args) {

        Scanner sc = new Scanner(System.in);

        Driver driver = new eg.edu.alexu.csd.oop.db.cs30.jdbc.Driver();
        Properties info = new Properties();

        System.out.println("Please Enter the Path of DataBase: ");
        File dbDir = new File(sc.nextLine());
        info.put("path", dbDir.getAbsoluteFile());
        java.sql.Connection connection;
        Statement statement;
        try {
            connection = driver.connect("jdbc:xmldb://localhost", info);
            Logger logger = ((DataBaseGenerator)DataBaseGenerator.makeInstance()).getMyLogger();
            logger.info("Driver is installed");
            logger.info("Connection is established with url jdbc:xmldb://localhost");
            logger.info("Statement is Created");
            statement = connection.createStatement();

            // Take user input
            String queryString = "";

            // Create and evaluate a query
            while (true) {
                System.out.print("Insert a query (or q to quit): ");

                try {
                    if (sc.hasNextLine()) {
                        queryString = sc.nextLine();
                    }

                    if (queryString.equalsIgnoreCase("q")) {
                        break;
                    }
                    System.out.println();

                    Query query = QueryBuilder.buildQuery(queryString.toLowerCase());
                    query.execute(statement, queryString.toLowerCase());
                } catch (SQLException e) {
                    System.out.println(e.getMessage());
                    logger.severe(e.getMessage());
                } catch (NullPointerException e) {
                    System.out.println("Error: There's no database");
                    logger.severe("Error: There's no database");
                } catch (Exception e) {
                    System.out.println("Error: there's something incorrect");
                    logger.severe("Error: there's something incorrect");
                }

                System.out.println();
            }
        } catch (SQLException e) {
            System.out.println("can't establish a connection");

        }
    }
}
