package eg.edu.alexu.csd.oop.test.jdbc;
import static org.junit.Assert.fail;

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


public class SanityTest {
   
    private Connection createDatabase(String databaseName, boolean drop) throws SQLException{
        Driver driver = (Driver)eg.edu.alexu.csd.oop.TestRunner.getImplementationInstanceForInterface(Driver.class);
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
    public void testWrongQuery() throws SQLException 
    {
        Connection connection = createDatabase("TestDB", true);
        
        try {
            Statement statement = connection.createStatement();
            statement.execute("CREATE   TABLE   table_name1(column_name1 varchar , column_name2    int,  column_name3 varchar)       ");
            statement.close();
        } catch (Throwable e){
            TestRunner.fail("Failed to parse query with extra spaces", e);
        }
        
        try {
            Statement statement = connection.createStatement();
            statement.execute("CREATE TABLE table_name2(column_name1 varchar, column_name2 int, column_name3 varchar)");
            statement.executeUpdate("INSERT INTO * table_name2(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            statement.close();
            fail("Bad query succeeded!");
        } catch (Throwable e){
        }
        
        try {
            Statement statement = connection.createStatement();
            statement.execute("CREATE TABLE table_name3(column_name1 varchar, column_name2 int, column_name3 varchar)");
            statement.executeUpdate("INS ERT INTO table_name3(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            statement.close();
            fail("Bad query succeeded!");
        } catch (Throwable e){
        }
        
        try {
            Statement statement = connection.createStatement();
            statement.execute("CREATE TABLE table_name4(column_name1 varchar, column_name2 int, column_name3 varchar)");
            statement.executeUpdate("INSERT INTO table_name4(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            statement.executeUpdate("UPDATE table_name4 SET column_name1=='1111111111', COLUMN_NAME2=2222222, column_name3='333333333'");
            statement.close();
            fail("Bad query succeeded!");
        } catch (Throwable e){
        }
        
        try {
            Statement statement = connection.createStatement();
            statement.execute("CREATE TABLE table_name6(column_name1 varchar, column_name2 int, column_name3 varchar)");
            statement.executeUpdate("INSERT INTO table_name6(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            statement.executeUpdate("UPDATE table_name6 SET column_name1='1111111111', COLUMN_NAME2=2222222, column_name3='333333333");
            statement.close();
            fail("Bad query succeeded!");
        } catch (Throwable e){
        }
        
        try {
            Statement statement = connection.createStatement();
            statement.execute("CREATE TABLE table_name7(column_name1 varchar, column_name2 int, column_name3 varchar)");
            statement.executeUpdate("INSERT INTO table_name7(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4");
            statement.close();
            fail("Bad query succeeded!");
        } catch (Throwable e){
        }
        
        try {
            Statement statement = connection.createStatement();
            statement.execute("CREATE TABLE table_name9(column_name1 varchar, column_name2 int, column_name3 varchar)");
            statement.executeUpdate("INSERT INTO ta.ble_name9(column_NAME1, COLUMN_name3, column_name.2) VALUES ('value1', 'value3', 4)");
            statement.close();
            fail("Query contains bad names and it succeeded!");
        } catch (Throwable e){
        }
        
        try {
            Statement statement = connection.createStatement();
            statement.executeUpdate("INSERT INTO table_name10(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            statement.close();
            fail("Inserting into not exiting table succeeded!");
        } catch (Throwable e){
        }
        
        connection.close();
    }
    
    @Test
    public void testCaseInsensitive() throws SQLException
    {
        Connection connection = createDatabase("TestDB", true);
        
        try {
            Statement statement = connection.createStatement();
            statement.execute("Create TABLE table_name1(column_name1 varchar, column_name2 int, column_name3 varchar)");
            
            int count1 = statement.executeUpdate("INSERT INTO table_name1(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count1);
            int count2 = statement.executeUpdate("INSERt INTO table_name1(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count2);
            int count3 = statement.executeUpdate("INSERT INTO table_name1(column_name1, COLUMN_NAME3, column_NAME2) VAlUES ('value2', 'value4', 5)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count3);
            int count4 = statement.executeUpdate("UPDATE table_namE1 SET column_name1='11111111', COLUMN_NAME2=22222222, column_name3='333333333' WHERE coLUmn_NAME3='VALUE3'");
            Assert.assertEquals("Updated returned wrong number", count1+count2, count4);
            statement.close();
        } catch (Throwable e){
            TestRunner.fail("Failed to update table with mixing capital and small letters!", e);
        }
        
        connection.close();
    }
    
    @Test
    public void testScenario_1() throws SQLException
    {
        Connection connection = createDatabase("TestDB", true);
        Statement statement = connection.createStatement();
        
        try {
            statement.execute("CREATE TABLE table_name1(column_name1 varchar, column_name2 int, column_name3 varchar)");
            
            int count1 = statement.executeUpdate("INSERT INTO table_name1(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count1);
            int count2 = statement.executeUpdate("INSERT INTO table_name1(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count2);
            int count3 = statement.executeUpdate("INSERT INTO table_name1(column_name1, COLUMN_NAME3, column_NAME2) VALUES ('value2', 'value4', 5)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count3);
            
            boolean created2 = statement.execute("DROP TABLE table_name1");
            Assert.assertEquals("Failed to drop table", true, created2);
        } catch (Throwable e){
            TestRunner.fail("Failed to complete scenario 1", e);
        }
        
        try {
            statement.executeUpdate("UPDATE table_name1 SET column_name1='11111111', COLUMN_NAME2=22222222, column_name3='333333333' WHERE coLUmn_NAME3='VALUE3'");
            fail("Update records from dropped table passed!");
        } catch (Throwable e) {
        }
        
        statement.close();
        connection.close();
    }
    
    @Test
    public void testScenario_2() throws SQLException
    {
        Connection connection = createDatabase("TestDB", true);
        
        try {
            Statement statement = connection.createStatement();
            statement.execute("CREATE TABLE table_name2(column_name1 varchar, column_name2 int, column_name3 varchar)");
            
            int count1 = statement.executeUpdate("INSERT INTO table_name2(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count1);
            int count2 = statement.executeUpdate("INSERT INTO table_name2(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 5)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count2);
            int count3 = statement.executeUpdate("INSERT INTO table_name2(column_name1, COLUMN_NAME3, column_NAME2) VALUES ('value2', 'value4', 6)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count3);
            
            int count4 = statement.executeUpdate("DELETE From table_name2  WHERE coLUmn_NAME1='VAluE1'");
            Assert.assertEquals("Delete returned wrong number", 2, count4);
            
            int count5 = statement.executeUpdate("UPDATE table_name2 SET column_name1='11111111', COLUMN_NAME2=22222222, column_name3='333333333' WHERE coLUmn_NAME2=4");
            Assert.assertEquals("Update returned wrong number", 0, count5);
            
            statement.close();
        } catch (Throwable e){
            TestRunner.fail("Failed to complete scenario 2", e);
        }
        
        connection.close();
    }
    
    @Test
    public void testScenario_3() throws SQLException
    {
        Connection connection = createDatabase("TestDB", true);
        
        try {
            Statement statement = connection.createStatement();
            statement.execute("CREATE TABLE table_name1(column_name1 varchar, column_name2 int, column_name3 varchar)");
            
            int count1 = statement.executeUpdate("INSERT INTO table_name1(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count1);
            int count2 = statement.executeUpdate("INSERT INTO table_name1(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 5)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count2);
            int count3 = statement.executeUpdate("INSERT INTO table_name1(column_name1, COLUMN_NAME3, column_NAME2) VALUES ('value2', 'value4', 6)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count3);
            
            int count4 = statement.executeUpdate("DELETE From table_name1  WHERE coLUmn_NAME2=4");
            Assert.assertEquals("Delete returned wrong number", 1, count4);
            
            ResultSet result = statement.executeQuery("SELECT * FROM table_name1 WHERE coluMN_NAME2 < 6");
            int rows = 0;
            while(result.next())    rows++;
            Assert.assertNotNull("Null result returned", result);
            Assert.assertEquals("Wrong number of rows", 1, rows);
            Assert.assertEquals("Wrong number of columns", 3, result.getMetaData().getColumnCount());
            
            int count5 = statement.executeUpdate("UPDATE table_name1 SET column_name1='11111111', COLUMN_NAME2=10, column_name3='333333333' WHERE coLUmn_NAME2=5");
            Assert.assertEquals("Update returned wrong number", 1, count5);
            
            ResultSet result2 = statement.executeQuery("SELECT * FROM table_name1 WHERE coluMN_NAME2 > 4");
            int rows2 = 0;
            while(result2.next())   rows2++;
            Assert.assertNotNull("Null result returned", result2);
            Assert.assertEquals("Wrong number of rows", 2, rows2);
            Assert.assertEquals("Wrong number of columns", 3, result2.getMetaData().getColumnCount());
            
            //Object column_2_object = result2[0][1];
            while(result2.previous());
            result2.next();
            Object column_2_object = result2.getObject(2);
            if (column_2_object instanceof String)
                fail("This should be 'Integer', but found 'String'!");
            else if (column_2_object instanceof Integer) {
                int column_2 = (Integer) column_2_object;
                Assert.assertEquals("Select did't return the updated record!", 10, column_2);
            }
            else
                fail("This should be 'Integer', but what is found can't be identified!");
            
            statement.close();
        } catch (Throwable e){
            TestRunner.fail("Failed to complete scenario 3", e);
        }
        
        connection.close();
    }
}
