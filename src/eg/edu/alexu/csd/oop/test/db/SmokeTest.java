package eg.edu.alexu.csd.oop.test.db;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Test;

import eg.edu.alexu.csd.oop.db.Database;
import eg.edu.alexu.csd.oop.test.TestRunner;

public class SmokeTest {

    public static Class<?> getSpecifications(){
        return Database.class;
    }
    
    private File createDatabase(Database db, String name, boolean drop){
        String path = db.createDatabase("sample" + System.getProperty("file.separator") + ((int)(Math.random() * 100000)) + System.getProperty("file.separator") + name, drop); // create database
        //System.out.println(path);
        Assert.assertNotNull("Failed to create database", path);
        File dbDir = new File(path);
        Assert.assertTrue("Database directory is not found or not a directory", dbDir.exists() && dbDir.isDirectory());
        return dbDir;
    }
    
    private File createDatabase_static(Database db, String name, boolean drop){
        String path = db.createDatabase("sample" + System.getProperty("file.separator") + name, drop);  // create database
        Assert.assertNotNull("Failed to create database", path);
        File dbDir = new File(path);
        Assert.assertTrue("Database directory is not found or not a directory", dbDir.exists() && dbDir.isDirectory());
        return dbDir;
    }
    
    @Test
    public void testCreateAndOpenAndDropDatabase() {
        File dummy = null;
        Database db = (Database)TestRunner.getImplementationInstanceForInterface(Database.class);
        {
            File dbDir = createDatabase_static(db, "SaMpLe", true);
            String files[] = dbDir.list();
            Assert.assertTrue("Database directory is not empty!", files == null || files.length == 0);
            dummy = new File(dbDir, "dummy");
            try {
                boolean created = dummy.createNewFile();
                Assert.assertTrue("Failed t create file into DB", created && dummy.exists());
            } catch (IOException e) {
                TestRunner.fail("Failed t create file into DB", e);
            }
        }
        {
            File dbDir = createDatabase_static(db, "sAmPlE", false);
            String files[] = dbDir.list();
            Assert.assertTrue("Database directory is empty after opening! Database name is case insensitive!", files.length > 0);
            Assert.assertTrue("Failed t create find a previously created file into DB", dummy.exists());
        }
        {
            File dbDir = createDatabase_static(db, "SAMPLE", true);
            String files[] = dbDir.list();
            Assert.assertTrue("Database directory is not empty after drop! Database name is case insensitive!", files == null || files.length == 0);
        }
    }

    @Test
    public void testCreateTable() {
    	Database db = (Database)TestRunner.getImplementationInstanceForInterface(Database.class);
        File dbDir = createDatabase(db, "TestDB_Create", true);
        try {
            String filesBefore[] = dbDir.list();
            db.executeStructureQuery("CREATE TABLE table_name1(column_name1 varchar, column_name2 int, column_name3 varchar)");
            String filesAfter[] = dbDir.list();
            Assert.assertNotEquals("Database directory contents weren't changed after table create", filesBefore.length, filesAfter.length);
        } catch (Throwable e){
            TestRunner.fail("Failed to create table", e);
        }
        try {
            boolean created = db.executeStructureQuery("CREATE TABLE table_name1(column_name1 varchar, column_name2 int, column_name3 varchar)");
            Assert.assertFalse("Create table succeed when table already exists", created);
        } catch (Throwable e){
            TestRunner.fail("Failed to create existing table", e);
        }
        try {
            db.executeStructureQuery("CREATE TABLE incomplete_table_name1");
            Assert.fail("Create invalid table succeed");
        } catch(SQLException e){
        } catch (Throwable e){
            TestRunner.fail("Unknown Exception thrown", e);
        }

    }
    
    @Test
    public void testCreateTableWithoutDB() {
    	Database db = (Database)TestRunner.getImplementationInstanceForInterface(Database.class);
        try {
            db.executeStructureQuery("CREATE TABLE table_name2(column_name1 varchar, column_name2 int, column_name3 varchar)");
            Assert.fail("Table created without database!!");
        } catch (Exception e) {
        }
    }

    @Test
    public void testInsertWithoutColumnNames() {
    	Database db = (Database)TestRunner.getImplementationInstanceForInterface(Database.class);
        createDatabase(db, "TestDB_Insert", true);
        try {
            db.executeStructureQuery("CREATE TABLE table_name3(column_name1 varchar, column_name2 int, column_name3 varchar)");
            int count = db.executeUpdateQuery("INSERT INTO table_name3 VALUES ('value1', 3,'value3')");
            Assert.assertNotEquals("Insert returned zero rows", 0, count);
        } catch (Throwable e){
            TestRunner.fail("Failed to insert into table", e);
        }
    }

    @Test
    public void testInsertWithColumnNames() {
    	Database db = (Database)TestRunner.getImplementationInstanceForInterface(Database.class);
        createDatabase(db, "TestDB_Insert", true);
        try {
            db.executeStructureQuery("CREATE TABLE table_name4(column_name1 varchar, column_name2 int, column_name3 varchar)");
            int count = db.executeUpdateQuery("INSERT INTO table_name4(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count);
        } catch (Throwable e){
            TestRunner.fail("Failed to insert into table", e);
        }
    }

    @Test
    public void testInsertWithWrongColumnNames() {
    	Database db = (Database)TestRunner.getImplementationInstanceForInterface(Database.class);
        createDatabase(db, "TestDB_Insert", true);
        try {
            db.executeStructureQuery("CREATE TABLE table_name5(column_name1 varchar, column_name2 int, column_name3 varchar)");
            db.executeUpdateQuery("INSERT INTO table_name5(invalid_column_name1, column_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.fail("Inserted with invalid column name!!");
        } catch (Throwable e){
        }
    }

    @Test
    public void testInsertWithWrongColumnCount() {
    	Database db = (Database)TestRunner.getImplementationInstanceForInterface(Database.class);
        createDatabase(db, "TestDB_Insert", true);
        try {
            db.executeStructureQuery("CREATE TABLE table_name6(column_name1 varchar, column_name2 int, column_name3 varchar)");
            db.executeUpdateQuery("INSERT INTO table_name6(column_name1, column_name2) VALUES ('value1', 4)");
            Assert.fail("Inserted with invalid column count!!");
        } catch (Throwable e){
        }
    }

    @Test
    public void testUpdate() {
    	Database db = (Database)TestRunner.getImplementationInstanceForInterface(Database.class);
        createDatabase(db, "TestDB_Update", true);
        try {
            db.executeStructureQuery("CREATE TABLE table_name7(column_name1 varchar, column_name2 int, column_name3 varchar)");
            int count1 = db.executeUpdateQuery("INSERT INTO table_name7(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count1);
            int count2 = db.executeUpdateQuery("INSERT INTO table_name7(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count2);
            int count3 = db.executeUpdateQuery("INSERT INTO table_name7(column_name1, COLUMN_NAME3, column_NAME2) VALUES ('value2', 'value4', 5)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count3);
            int count4 = db.executeUpdateQuery("UPDATE table_name7 SET column_name1='1111111111', COLUMN_NAME2=2222222, column_name3='333333333'");
            Assert.assertEquals("Updated returned wrong number", count1+count2+count3, count4);
        } catch (Throwable e){
            TestRunner.fail("Failed to update table", e);
        }
    }
    
    @Test
    public void testConditionalUpdate() {
    	Database db = (Database)TestRunner.getImplementationInstanceForInterface(Database.class);
        createDatabase(db, "TestDB_Update", true);
        try {
            db.executeStructureQuery("CREATE TABLE table_name8(column_name1 varchar, column_name2 int, column_name3 varchar)");
            int count1 = db.executeUpdateQuery("INSERT INTO table_name8(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count1);
            int count2 = db.executeUpdateQuery("INSERT INTO table_name8(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count2);
            int count3 = db.executeUpdateQuery("INSERT INTO table_name8(column_name1, COLUMN_NAME3, column_NAME2) VALUES ('value2', 'value4', 5)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count3);
            int count4 = db.executeUpdateQuery("UPDATE table_name8 SET column_name1='11111111', COLUMN_NAME2=22222222, column_name3='333333333' WHERE coLUmn_NAME3='VALUE3'");
            Assert.assertEquals("Updated returned wrong number", count1+count2, count4);
        } catch (Throwable e){
            TestRunner.fail("Failed to update table", e);
        }
    }

    @Test
    public void testUpdateEmptyOrInvalidTable() {
    	Database db = (Database)TestRunner.getImplementationInstanceForInterface(Database.class);
        createDatabase(db, "TestDB_Update", true);
        try {
            db.executeStructureQuery("CREATE TABLE table_name9(column_name1 varchar, column_name2 int, column_name3 varchar)");
            int count = db.executeUpdateQuery("UPDATE table_name9 SET column_name1='value1', column_name2=15, column_name3='value2'");
            Assert.assertEquals("Updated empty table retruned non-zero count!", 0, count);
        } catch (Throwable e){
            TestRunner.fail("Failed to update table", e);
        }
        try {
            db.executeUpdateQuery("UPDATE wrong_table_name9 SET column_name1='value1', column_name2=15, column_name3='value2'");
            Assert.fail("Updated empty table retruned non-zero count!");
        } catch (SQLException e){
        } catch (Throwable e) {
            TestRunner.fail("Invalid exception was thrown", e);
        }
    }
    
    @Test
    public void testDelete() {
    	Database db = (Database)TestRunner.getImplementationInstanceForInterface(Database.class);
        createDatabase(db, "TestDB_Update", true);
        try {
            db.executeStructureQuery("CREATE TABLE table_name10(column_name1 varchar, column_name2 int, column_name3 varchar)");
            int count1 = db.executeUpdateQuery("INSERT INTO table_name10(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count1);
            int count2 = db.executeUpdateQuery("INSERT INTO table_name10(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count2);
            int count3 = db.executeUpdateQuery("INSERT INTO table_name10(column_name1, COLUMN_NAME3, column_NAME2) VALUES ('value2', 'value4', 5)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count3);
            int count4 = db.executeUpdateQuery("DELETE From table_name10");
            Assert.assertEquals("Delete returned wrong number", 3, count4);
        } catch (Throwable e){
            TestRunner.fail("Failed to delete from table", e);
        }
    }
    
    @Test
    public void testConditionalDelete() {
    	Database db = (Database)TestRunner.getImplementationInstanceForInterface(Database.class);
        createDatabase(db, "TestDB_Update", true);
        try {
            db.executeStructureQuery("CREATE TABLE table_name11(column_name1 varchar, column_name2 int, column_name3 varchar)");
            int count1 = db.executeUpdateQuery("INSERT INTO table_name11(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count1);
            int count2 = db.executeUpdateQuery("INSERT INTO table_name11(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count2);
            int count3 = db.executeUpdateQuery("INSERT INTO table_name11(column_name1, COLUMN_NAME3, column_NAME2) VALUES ('value2', 'value4', 5)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count3);
            int count4 = db.executeUpdateQuery("DELETE From table_name11  WHERE coLUmn_NAME3='VAluE3'");
            Assert.assertEquals("Delete returned wrong number", 2, count4);
        } catch (Throwable e){
            TestRunner.fail("Failed to delete from table", e);
        }
    }

    @Test
    public void testSelect() {
    	Database db = (Database)TestRunner.getImplementationInstanceForInterface(Database.class);
        createDatabase(db, "TestDB_Select", true);
        try {
            db.executeStructureQuery("CREATE TABLE table_name12(column_name1 varchar, column_name2 int, column_name3 varchar)");
            int count1 = db.executeUpdateQuery("INSERT INTO table_name12(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count1);
            int count2 = db.executeUpdateQuery("INSERT INTO table_name12(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count2);
            int count3 = db.executeUpdateQuery("INSERT INTO table_name12(column_name1, COLUMN_NAME3, column_NAME2) VALUES ('value2', 'value4', 5)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count3);
            int count4 = db.executeUpdateQuery("INSERT INTO table_name12(column_name1, COLUMN_NAME3, column_NAME2) VALUES ('value5', 'value6', 6)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count4);
            Object[][] result = db.executeQuery("SELECT * From table_name12");
            Assert.assertNotNull("Null result retruned", result);
            Assert.assertEquals("Wrong number of rows", 4, result.length);
            Assert.assertEquals("Wrong number of columns", 3, result[0].length);
        } catch (Throwable e){
            TestRunner.fail("Failed to select from table", e);
        }
    }
    
    @Test
    public void testConditionalSelect() {
    	Database db = (Database)TestRunner.getImplementationInstanceForInterface(Database.class);
        createDatabase(db, "TestDB_Select", true);
        try {
            db.executeStructureQuery("CREATE TABLE table_name13(column_name1 varchar, column_name2 int, column_name3 varchar)");
            int count1 = db.executeUpdateQuery("INSERT INTO table_name13(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count1);
            int count2 = db.executeUpdateQuery("INSERT INTO table_name13(column_NAME1, column_name2, COLUMN_name3) VALUES ('value1', 4, 'value3')");
            Assert.assertNotEquals("Insert returned zero rows", 0, count2);
            int count3 = db.executeUpdateQuery("INSERT INTO table_name13(column_name1, COLUMN_NAME3, column_NAME2) VALUES ('value2', 'value4', 5)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count3);
            int count4 = db.executeUpdateQuery("INSERT INTO table_name13(column_name1, COLUMN_NAME3, column_NAME2) VALUES ('value5', 'value6', 6)");
            Assert.assertNotEquals("Insert returned zero rows", 0, count4);
            Object[][] result = db.executeQuery("SELECT column_name1 FROM table_name13 WHERE coluMN_NAME2 < 5");
            Assert.assertNotNull("Null result retruned", result);
            Assert.assertEquals("Wrong number of rows", 2, result.length);
            Assert.assertEquals("Wrong number of columns", 1, result[0].length);
        } catch (Throwable e){
            TestRunner.fail("Failed to select from table", e);
        }
    }
    //*/
}
