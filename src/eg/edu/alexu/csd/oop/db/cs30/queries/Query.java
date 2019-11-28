package eg.edu.alexu.csd.oop.db.cs30.queries;

import eg.edu.alexu.csd.oop.db.Database;

import java.sql.SQLException;

/**
 * Represents SQL query
 */
public interface Query {
    /**
     * Validate query
     */
    boolean isCorrect(String query);

    /**
     * @return query's id
     *      0: create database, 1: create table, 2: drop database, 3: drop table, 4: insert, 5: Delete, 6: Update, 7: select
     */
    int getId();

    /**
     * Execute query's operation, and print output
     */
    void execute(Database database, String query) throws SQLException;
}
