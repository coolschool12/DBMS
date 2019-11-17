package eg.edu.alexu.csd.oop.test.db;
import static org.junit.Assert.fail;

import java.io.File;

import org.junit.*;

import eg.edu.alexu.csd.oop.db.Database;
import eg.edu.alexu.csd.oop.test.TestRunner;

public class SanityTest {

	public static Class<?> getSpecifications(){
		return Database.class;
	}

	private File createDatabase(Database db, String name, boolean drop){
		String path = db.createDatabase("sample" + System.getProperty("file.separator") + name, drop); // create database
		//System.out.println(path);
		Assert.assertNotNull("Failed to create database", path);
		File dbDir = new File(path);
		Assert.assertTrue("Database directory is not found or not a directory", dbDir.exists() && dbDir.isDirectory());
		return dbDir;
	}

	@Test
	public void testWrongQuery() {
		Database db = (Database)TestRunner.getImplementationInstanceForInterface(Database.class);

		try {
			createDatabase(db, "TestDB", true);
			db.executeStructureQuery("CREATE   TABLE   table_name1(column_name1 varchar , column_name2    int,  column_name3 varchar)       ");
		} catch (Throwable e){
			TestRunner.fail("Failed to parse query with extra spaces", e);
		}

		try {
			createDatabase(db, "TestDB", true);
			db.executeUpdateQuery("INSERT INTO * table_name2(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
			fail("Bad query succeeded!");
		} catch (Throwable e){
		}

		try {
			createDatabase(db, "TestDB", true);
			db.executeUpdateQuery("INS ERT INTO table_name3(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
			fail("Bad query succeeded!");
		} catch (Throwable e){
		}

		try {
			createDatabase(db, "TestDB", true);
			db.executeStructureQuery("CREATE TABLE table_name4(column_name1 varchar, column_name2 int, column_name3 varchar)");
			db.executeUpdateQuery("INSERT INTO table_name4(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
			db.executeUpdateQuery("UPDATE table_name4 SET column_name1=='1111111111', COLUMN_NAME2=2222222, column_name3='333333333'");
			fail("Bad query succeeded!");
		} catch (Throwable e){
		}

		try {
			createDatabase(db, "TestDB", true);
			db.executeStructureQuery("CREATE TABLE table_name5(column_name1 varchar, column_name2 int, column_name3 varchar)");
			db.executeUpdateQuery("INSERT INTO table_name5(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
			db.executeUpdateQuery("UPDATE table_name5 SET column_name1='1111111111', COLUMN_NAME2='2222222', column_name3='333333333'");
			fail("Bad query succeeded!");
		} catch (Throwable e){
		}

		try {
			createDatabase(db, "TestDB", true);
			db.executeStructureQuery("CREATE TABLE table_name6(column_name1 varchar, column_name2 int, column_name3 varchar)");
			db.executeUpdateQuery("INSERT INTO table_name6(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
			db.executeUpdateQuery("UPDATE table_name6 SET column_name1='1111111111', COLUMN_NAME2=2222222, column_name3='333333333");
			fail("Bad query succeeded!");
		} catch (Throwable e){
		}

		try {
			createDatabase(db, "TestDB", true);
			db.executeStructureQuery("CREATE TABLE table_name7(column_name1 varchar, column_name2 int, column_name3 varchar)");
			db.executeUpdateQuery("INSERT INTO table_name7(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4");
			fail("Bad query succeeded!");
		} catch (Throwable e){
		}

		try {
			createDatabase(db, "TestDB", true);
			db.executeStructureQuery("CREATE TABLE table_name8(column_name1 varchar, column_name2 int, column_name3 varchar)");
			db.executeUpdateQuery("INSERT INTO table_name8(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 4, 'value2')");
			fail("Type mismatches succeeded!");
		} catch (Throwable e){
		}

		try {
			createDatabase(db, "TestDB", true);
			db.executeStructureQuery("CREATE TABLE table_name9(column_name1 varchar, column_name2 int, column_name3 varchar)");
			db.executeUpdateQuery("INSERT INTO ta.ble_name9(column_NAME1, COLUMN_name3, column_name.2) VALUES ('value1', 'value3', 4)");
			fail("Query contains bad names and it succeeded!");
		} catch (Throwable e){
		}


	}

	@Test
	public void testCaseInsensitive()
	{
		Database db = (Database)TestRunner.getImplementationInstanceForInterface(Database.class);
		createDatabase(db, "TestDB", true);

		try {
			db.executeStructureQuery("Create TABLE table_name1(column_name1 varchar, column_name2 int, column_name3 varchar)");
			int count1 = db.executeUpdateQuery("INSERT INTO table_name1(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
			Assert.assertNotEquals("Insert returned zero rows", 0, count1);
			int count2 = db.executeUpdateQuery("INSERt INTO table_name1(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
			Assert.assertNotEquals("Insert returned zero rows", 0, count2);
			int count3 = db.executeUpdateQuery("INSERT INTO table_name1(column_name1, COLUMN_NAME3, column_NAME2) VAlUES ('value2', 'value4', 5)");
			Assert.assertNotEquals("Insert returned zero rows", 0, count3);
			int count4 = db.executeUpdateQuery("UPDATE table_namE1 SET column_name1='11111111', COLUMN_NAME2=22222222, column_name3='333333333' WHERE coLUmn_NAME3='VALUE3'");
			Assert.assertEquals("Updated returned wrong number", count1+count2, count4);
		} catch (Throwable e){
			TestRunner.fail("Failed to update table with mixing capital and small letters!", e);
		}
	}

	@Test
	public void testScenario_1()
	{
		Database db = (Database)TestRunner.getImplementationInstanceForInterface(Database.class);

		try {
			boolean created = db.executeStructureQuery("CREATE DATABASE TestDB");
			Assert.assertEquals("Failed to create TestDB internally using query", true, created);

			db.executeStructureQuery("CREATE TABLE table_name1(column_name1 varchar, column_name2 int, column_name3 varchar)");

			int count1 = db.executeUpdateQuery("INSERT INTO table_name1(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
			Assert.assertNotEquals("Insert returned zero rows", 0, count1);
			int count2 = db.executeUpdateQuery("INSERT INTO table_name1(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
			Assert.assertNotEquals("Insert returned zero rows", 0, count2);
			int count3 = db.executeUpdateQuery("INSERT INTO table_name1(column_name1, COLUMN_NAME3, column_NAME2) VALUES ('value2', 'value4', 5)");
			Assert.assertNotEquals("Insert returned zero rows", 0, count3);

			boolean created2 = db.executeStructureQuery("DROP TABLE table_name1");
			Assert.assertEquals("Failed to drop table", true, created2);
		} catch (Throwable e){
			TestRunner.fail("Failed to complete scenario 1", e);
		}

		try {
			db.executeUpdateQuery("UPDATE table_name1 SET column_name1='11111111', COLUMN_NAME2=22222222, column_name3='333333333' WHERE coLUmn_NAME3='VALUE3'");
			fail("Update records from dropped table passed!");
		} catch (Throwable e) {
		}
	}

	@Test
	public void testScenario_2()
	{
		Database db = (Database)TestRunner.getImplementationInstanceForInterface(Database.class);

		try {
			boolean created = db.executeStructureQuery("CREATE DATABASE TestDB");
			Assert.assertEquals("Failed to create TestDB internally using query", true, created);

			boolean created2 = db.executeStructureQuery("DROP DATABASE TestDB");
			Assert.assertEquals("Failed to drop TestDB internally using query", true, created2);
		} catch (Throwable e){
			TestRunner.fail("Failed to complete scenario 2", e);
		}

		try {
			db.executeStructureQuery("CREATE TABLE table_name1(column_name1 varchar, column_name2 int, column_name3 varchar)");
			fail("Table created on a dropped database!");
		} catch (Throwable e) {
		}

		try {
			boolean created = db.executeStructureQuery("CREATE DATABASE TestDB");
			Assert.assertEquals("Failed to create TestDB internally using query", true, created);

			db.executeStructureQuery("CREATE TABLE table_name2(column_name1 varchar, column_name2 int, column_name3 varchar)");

			int count1 = db.executeUpdateQuery("INSERT INTO table_name2(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
			Assert.assertNotEquals("Insert returned zero rows", 0, count1);
			int count2 = db.executeUpdateQuery("INSERT INTO table_name2(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 5)");
			Assert.assertNotEquals("Insert returned zero rows", 0, count2);
			int count3 = db.executeUpdateQuery("INSERT INTO table_name2(column_name1, COLUMN_NAME3, column_NAME2) VALUES ('value2', 'value4', 6)");
			Assert.assertNotEquals("Insert returned zero rows", 0, count3);

			int count4 = db.executeUpdateQuery("DELETE From table_name2  WHERE coLUmn_NAME1='VAluE1'");
			Assert.assertEquals("Delete returned wrong number", 2, count4);

			int count5 = db.executeUpdateQuery("UPDATE table_name2 SET column_name1='11111111', COLUMN_NAME2=22222222, column_name3='333333333' WHERE coLUmn_NAME2=4");
			Assert.assertEquals("Update returned wrong number", 0, count5);
		} catch (Throwable e){
			TestRunner.fail("Failed to complete scenario 2", e);
		}

	}

	@Test
	public void testScenario_3()
	{
		Database db = (Database)TestRunner.getImplementationInstanceForInterface(Database.class);

		try {
			createDatabase(db, "TestDB", true);

			db.executeStructureQuery("CREATE TABLE table_name1(column_name1 varchar, column_name2 int, column_name3 varchar)");

			int count1 = db.executeUpdateQuery("INSERT INTO table_name1(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 4)");
			Assert.assertNotEquals("Insert returned zero rows", 0, count1);
			int count2 = db.executeUpdateQuery("INSERT INTO table_name1(column_NAME1, COLUMN_name3, column_name2) VALUES ('value1', 'value3', 5)");
			Assert.assertNotEquals("Insert returned zero rows", 0, count2);
			int count3 = db.executeUpdateQuery("INSERT INTO table_name1(column_name1, COLUMN_NAME3, column_NAME2) VALUES ('value2', 'value4', 6)");
			Assert.assertNotEquals("Insert returned zero rows", 0, count3);

			int count4 = db.executeUpdateQuery("DELETE From table_name1  WHERE coLUmn_NAME2=4");
			Assert.assertEquals("Delete returned wrong number", 1, count4);

			Object[][] result = db.executeQuery("SELECT * FROM table_name1 WHERE coluMN_NAME2 < 6");
			Assert.assertNotNull("Null result returned", result);
			Assert.assertEquals("Wrong number of rows", 1, result.length);
			Assert.assertEquals("Wrong number of columns", 3, result[0].length);

			int count5 = db.executeUpdateQuery("UPDATE table_name1 SET column_name1='11111111', COLUMN_NAME2=10, column_name3='333333333' WHERE coLUmn_NAME2=5");
			Assert.assertEquals("Update returned wrong number", 1, count5);

			Object[][] result2 = db.executeQuery("SELECT * FROM table_name1 WHERE coluMN_NAME2 > 4");
			Assert.assertNotNull("Null result returned", result2);
			Assert.assertEquals("Wrong number of rows", 2, result2.length);
			Assert.assertEquals("Wrong number of columns", 3, result2[0].length);
			Object column_2_object = result2[0][1];
			if (column_2_object instanceof String)
				fail("This should be 'Integer', but found 'String'!");
			else if (column_2_object instanceof Integer) {
				int column_2 = (Integer) column_2_object;
				Assert.assertEquals("Select did't return the updated record!", 10, column_2);
			}
			else
				fail("This should be 'Integer', but what is found can't be identified!");

		} catch (Throwable e){
			TestRunner.fail("Failed to complete scenario 3", e);
		}

	}
}
