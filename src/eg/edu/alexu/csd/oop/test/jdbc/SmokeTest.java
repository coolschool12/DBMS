package eg.edu.alexu.csd.oop.test.jdbc;
import java.io.File;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import eg.edu.alexu.csd.oop.TestRunner;


public class SmokeTest {

    public static Class<?> getSpecifications(){
        return Driver.class;
    }
    
    private Connection createDatabase(String databaseName, boolean drop) throws SQLException{
        Driver driver = (Driver)TestRunner.getImplementationInstanceForInterface(Driver.class);
        Properties info = new Properties();
        File dbDir = new File("sample" + System.getProperty("file.separator") + ((int)(Math.random() * 100000)));
        info.put("path", dbDir.getAbsoluteFile());
        Connection connection = driver.connect("jdbc:xmldb://localhost", info);
        Statement statement = connection.createStatement();
        statement.execute("DROP DATABASE " + databaseName);
        if(drop)
            statement.execute("CREATE DATABASE " + databaseName);
        statement.close();
        return connection;
    }
    
    @Test
    public void testCreateAndOpenAndDropDatabase() throws SQLException {
        Driver driver = (Driver)TestRunner.getImplementationInstanceForInterface(Driver.class);
        Properties info = new Properties();
        File dbDir = new File("sample" + System.getProperty("file.separator") + (Math.random() * 100000));
        info.put("path", dbDir.getAbsoluteFile());
        Connection connection = driver.connect("jdbc:xmldb://localhost", info);
        {
            Statement statement = connection.createStatement();
            statement.execute("DROP DATABASE SaMpLe");
            statement.execute("CREATE DATABASE SaMpLe");
            statement.execute("DROP DATABASE SaMpLe");
            String files[] = dbDir.list();
            Assert.assertTrue("Database directory is not empty!", files == null || files.length == 0);
            statement.close();
        }
        
        {
            Statement statement = connection.createStatement();
            statement.execute("DROP DATABASE SAMPLE");
            statement.execute("CREATE DATABASE SAMPLE");
            statement.execute("DROP DATABASE SAMPLE");
            String files[] = dbDir.list();
            Assert.assertTrue("Database directory is not empty after drop!", files == null || files.length == 0);
            statement.close();
        }
        connection.close();
    }

    @Test
    public void testCreateTable() throws SQLException {
        Connection connection = createDatabase("TestDB_Create", true);
        try {
            Statement statement = connection.createStatement();
            statement.execute("CREATE TABLE table_name1(column_name1 varchar, column_name2 int, column_name3 varchar)");
            statement.close();
        } catch (Throwable e){
            TestRunner.fail("Failed to create table", e);
        }
        try {
            Statement statement = connection.createStatement();
            boolean created = statement.execute("CREATE TABLE table_name1(column_name1 varchar, column_name2 int, column_name3 varchar)");
            Assert.assertFalse("Create table succeed when table already exists", created);
        } catch (Throwable e){
            TestRunner.fail("Failed to create existing table", e);
        }
        try {
            Statement statement = connection.createStatement();
            statement.execute("CREATE TABLE incomplete_table_name1");
            Assert.fail("Create invalid table succeed");
        } catch(SQLException e){
        } catch (Throwable e){
            TestRunner.fail("Unknown Exception thrown", e);
        }
        connection.close();
    }

    @Test
    public void testInsertWithoutColumnNames() throws SQLException {
        Connection connection = createDatabase("TestDB_Insert", true);
        try {
            Statement statement = connection.createStatement();
            statement.execute("CREATE TABLE table_name3(column_name1 varchar, column_name2 int, column_name3 varchar)");
            int count = statement.executeUpdate("INSERT INTO table_name3 VALUES ('value1', 3,'value3')");
            Assert.assertNotEquals("Insert returned zero rows", 0, count);
            statement.close();
        } catch (Throwable e){
            TestRunner.fail("Failed to insert into table", e);
        }
        connection.close();
    }

    @Test
    public void testInsertWithColumnNames() throws SQLException {
        Connection connection = createDatabase("TestDB_Insert", true);
        try {
            Statement statement = connection.createStatement();
            statement.execute("CREATE TABLE table_name4(column_name1 varchar, column_name2 int, column_name3 varchar)");
            int count = statement.executeUpdate("INSERT INTO table_name4(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count);
            statement.close();
        } catch (Throwable e){
            TestRunner.fail("Failed to insert into table", e);
        }
        connection.close();
    }

    @Test
    public void testInsertWithWrongColumnNames() throws SQLException {
        Connection connection = createDatabase("TestDB_Insert", true);
        try {
            Statement statement = connection.createStatement();
            statement.execute("CREATE TABLE table_name5(column_name1 varchar, column_name2 int, column_name3 varchar)");
            statement.executeUpdate("INSERT INTO table_name5(invalid_column_name1, column_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.fail("Inserted with invalid column name!!");
            statement.close();
        } catch (Throwable e){
        }
        connection.close();
    }

    @Test
    public void testInsertWithWrongColumnCount() throws SQLException {
        Connection connection = createDatabase("TestDB_Insert", true);
        try {
            Statement statement = connection.createStatement();
            statement.execute("CREATE TABLE table_name6(column_name1 varchar, column_name2 int, column_name3 varchar)");
            statement.executeUpdate("INSERT INTO table_name6(column_name1, column_name2) VALUES ('value1', 4)");
            Assert.fail("Inserted with invalid column count!!");
            statement.close();
        } catch (Throwable e){
        }
        connection.close();
    }

    @Test
    public void testUpdate() throws SQLException {
        Connection connection = createDatabase("TestDB_Update", true);
        try {
            Statement statement = connection.createStatement();
            statement.execute("CREATE TABLE table_name7(column_name1 varchar, column_name2 int, column_name3 varchar)");
            int count1 = statement.executeUpdate("INSERT INTO table_name7(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count1);
            int count2 = statement.executeUpdate("INSERT INTO table_name7(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count2);
            int count3 = statement.executeUpdate("INSERT INTO table_name7(column_name1, COLUMN_NAME3, column_NAME2) VALUES ('value2', 'value4', 5)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count3);
            int count4 = statement.executeUpdate("UPDATE table_name7 SET column_name1='1111111111', COLUMN_NAME2=2222222, column_name3='333333333'");
            Assert.assertEquals("Updated returned wrong number", count1+count2+count3, count4);
            statement.close();
        } catch (Throwable e){
            TestRunner.fail("Failed to update table", e);
        }
        connection.close();
    }
    
    @Test
    public void testConditionalUpdate() throws SQLException {
        Connection connection = createDatabase("TestDB_Update", true);
        try {
            Statement statement = connection.createStatement();
            statement.execute("CREATE TABLE table_name8(column_name1 varchar, column_name2 int, column_name3 varchar)");
            int count1 = statement.executeUpdate("INSERT INTO table_name8(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count1);
            int count2 = statement.executeUpdate("INSERT INTO table_name8(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count2);
            int count3 = statement.executeUpdate("INSERT INTO table_name8(column_name1, COLUMN_NAME3, column_NAME2) VALUES ('value2', 'value4', 5)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count3);
            int count4 = statement.executeUpdate("UPDATE table_name8 SET column_name1='11111111', COLUMN_NAME2=22222222, column_name3='333333333' WHERE coLUmn_NAME3='VALUE3'");
            Assert.assertEquals("Updated returned wrong number", count1+count2, count4);
            statement.close();
        } catch (Throwable e){
            TestRunner.fail("Failed to update table", e);
        }
        connection.close();
    }

    @Test
    public void testUpdateEmptyOrInvalidTable() throws SQLException {
        Connection connection = createDatabase("TestDB_Update", true);
        try {
            Statement statement = connection.createStatement();
            statement.execute("CREATE TABLE table_name9(column_name1 varchar, column_name2 int, column_name3 varchar)");
            int count = statement.executeUpdate("UPDATE table_name9 SET column_name1='value1', column_name2=15, column_name3='value2'");
            Assert.assertEquals("Updated empty table retruned non-zero count!", 0, count);
            statement.close();
        } catch (Throwable e){
            TestRunner.fail("Failed to update table", e);
        }
        try {
            Statement statement = connection.createStatement();
            statement.executeUpdate("UPDATE wrong_table_name9 SET column_name1='value1', column_name2=15, column_name3='value2'");
            Assert.fail("Updated empty table retruned non-zero count!");
            statement.close();
        } catch (SQLException e){
        } catch (Throwable e) {
            TestRunner.fail("Invalid exception was thrown", e);
        }
        connection.close();
    }
    
    @Test
    public void testDelete() throws SQLException {
        Connection connection = createDatabase("TestDB_Update", true);
        try {
            Statement statement = connection.createStatement();
            statement.execute("CREATE TABLE table_name10(column_name1 varchar, column_name2 int, column_name3 varchar)");
            int count1 = statement.executeUpdate("INSERT INTO table_name10(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count1);
            int count2 = statement.executeUpdate("INSERT INTO table_name10(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count2);
            int count3 = statement.executeUpdate("INSERT INTO table_name10(column_name1, COLUMN_NAME3, column_NAME2) VALUES ('value2', 'value4', 5)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count3);
            int count4 = statement.executeUpdate("DELETE From table_name10");
            Assert.assertEquals("Delete returned wrong number", 3, count4);
            statement.close();
        } catch (Throwable e){
            TestRunner.fail("Failed to delete from table", e);
        }
        connection.close();
    }
    
    @Test
    public void testConditionalDelete() throws SQLException {
        Connection connection = createDatabase("TestDB_Update", true);
        try {
            Statement statement = connection.createStatement();
            statement.execute("CREATE TABLE table_name11(column_name1 varchar, column_name2 int, column_name3 varchar)");
            int count1 = statement.executeUpdate("INSERT INTO table_name11(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count1);
            int count2 = statement.executeUpdate("INSERT INTO table_name11(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count2);
            int count3 = statement.executeUpdate("INSERT INTO table_name11(column_name1, COLUMN_NAME3, column_NAME2) VALUES ('value2', 'value4', 5)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count3);
            int count4 = statement.executeUpdate("DELETE From table_name11  WHERE coLUmn_NAME3='VAluE3'");
            Assert.assertEquals("Delete returned wrong number", 2, count4);
            statement.close();
        } catch (Throwable e){
            TestRunner.fail("Failed to delete from table", e);
        }
        connection.close();
    }

    @Test
    public void testSelect() throws SQLException {
        Connection connection = createDatabase("TestDB_Select", true);
        try {
            Statement statement = connection.createStatement();
            statement.execute("CREATE TABLE table_name12(column_name1 varchar, column_name2 int, column_name3 varchar)");
            int count1 = statement.executeUpdate("INSERT INTO table_name12(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count1);
            int count2 = statement.executeUpdate("INSERT INTO table_name12(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count2);
            int count3 = statement.executeUpdate("INSERT INTO table_name12(column_name1, COLUMN_NAME3, column_NAME2) VALUES ('value2', 'value4', 5)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count3);
            int count4 = statement.executeUpdate("INSERT INTO table_name12(column_name1, COLUMN_NAME3, column_NAME2) VALUES ('value5', 'value6', 6)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count4);
            ResultSet result = statement.executeQuery("SELECT * From table_name12");
            int rows = 0;
            while(result.next())    rows++;
            Assert.assertNotNull("Null result retruned", result);
            Assert.assertEquals("Wrong number of rows", 4, rows);
            Assert.assertEquals("Wrong number of columns", 3, result.getMetaData().getColumnCount());
            statement.close();
        } catch (Throwable e){
            TestRunner.fail("Failed to select from table", e);
        }
        connection.close();
    }
    
    @Test
    public void testConditionalSelect() throws SQLException {
        Connection connection = createDatabase("TestDB_Select", true);
        try {
            Statement statement = connection.createStatement();
            statement.execute("CREATE TABLE table_name13(column_name1 varchar, column_name2 int, column_name3 varchar)");
            int count1 = statement.executeUpdate("INSERT INTO table_name13(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count1);
            int count2 = statement.executeUpdate("INSERT INTO table_name13(column_NAME1, column_name2, COLUMN_name3) VALUES ('value1', 4, 'value3')");
            Assert.assertNotEquals("Insert returned zero rows", 0, count2);
            int count3 = statement.executeUpdate("INSERT INTO table_name13(column_name1, COLUMN_NAME3, column_NAME2) VALUES ('value2', 'value4', 5)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count3);
            int count4 = statement.executeUpdate("INSERT INTO table_name13(column_name1, COLUMN_NAME3, column_NAME2) VALUES ('value5', 'value6', 6)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count4);
            ResultSet result = statement.executeQuery("SELECT column_name1 FROM table_name13 WHERE coluMN_NAME2 < 5");
            int rows = 0;
            while(result.next())    rows++;
            Assert.assertNotNull("Null result retruned", result);
            Assert.assertEquals("Wrong number of rows", 2, rows);
            Assert.assertEquals("Wrong number of columns", 1, result.getMetaData().getColumnCount());
            statement.close();
        } catch (Throwable e){
            TestRunner.fail("Failed to select from table", e);
        }
        connection.close();
    }

    @Test
    public void testExecute() throws SQLException {
        Connection connection = createDatabase("TestDB_Select", true);
        try {
            Statement statement = connection.createStatement();
            statement.execute("CREATE TABLE table_name13(column_name1 varchar, column_name2 int, column_name3 varchar)");
            int count1 = statement.executeUpdate("INSERT INTO table_name13(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count1);
            boolean result1 = statement.execute("INSERT INTO table_name13(column_NAME1, column_name2, COLUMN_name3) VALUES ('value1', 8, 'value3')");
            Assert.assertTrue("Wrong return for insert record", result1);
            int count3 = statement.executeUpdate("INSERT INTO table_name13(column_name1, COLUMN_NAME3, column_NAME2) VALUES ('value2', 'value4', 5)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count3);
            int count4 = statement.executeUpdate("INSERT INTO table_name13(column_name1, COLUMN_NAME3, column_NAME2) VALUES ('value5', 'value6', 6)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count4);
            boolean result2 = statement.execute("SELECT column_name1 FROM table_name13 WHERE coluMN_NAME2 = 8");
            Assert.assertTrue("Wrong return for select existing records", result2);
            boolean result3 = statement.execute("SELECT column_name1 FROM table_name13 WHERE coluMN_NAME2 > 100");
            Assert.assertFalse("Wrong return for select non existing records", result3);
            statement.close();
        } catch (Throwable e){
            TestRunner.fail("Failed to select from table", e);
        }
        connection.close();
    }
}
