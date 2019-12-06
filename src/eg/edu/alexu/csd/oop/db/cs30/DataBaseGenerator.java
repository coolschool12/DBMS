package eg.edu.alexu.csd.oop.db.cs30;

import eg.edu.alexu.csd.oop.db.Database;
import eg.edu.alexu.csd.oop.db.cs30.queries.ExtractData;
import eg.edu.alexu.csd.oop.db.cs30.queries.Query;
import eg.edu.alexu.csd.oop.db.cs30.queries.QueryBuilder;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Logger;

public class DataBaseGenerator implements Database {

    private ArrayList<MyDataBase> dataBases;
    private MyDataBase activeDataBase;
    private ExtractData extractData;
    private String pathToDatabases = "databases" + System.getProperty("file.separator") + ((int)(Math.random() * 100));
    private static Database database = null;
    private Logger myLogger;
    private static String[] selectedColumnNames;
    private static Integer[] selectedColumnTypes;

    private DataBaseGenerator()
    {
        extractData = ExtractData.makeInstance();
        dataBases = new ArrayList<>();
        System.setProperty("java.util.logging.config.file", "logging.properties");
        myLogger=Logger.getLogger(DataBaseGenerator.class.getName());
    }

    public static Database makeInstance()
    {
        if (database == null)
            database = new DataBaseGenerator();

        return database;
    }

    public void setPathToDatabases(String pathToDatabases) {
        this.pathToDatabases = pathToDatabases;
    }

    @Override
    public String createDatabase(String databaseName, boolean dropIfExists) {

        boolean isFound = searchForThatName(databaseName);

        if(isFound && dropIfExists)
        {
            try {
                executeStructureQuery("DROP DATABASE " + databaseName);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (!(isFound && !dropIfExists))
        {
            try {
                executeStructureQuery("CREATE DATABASE " + databaseName);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return activeDataBase.getPath();
    }

    @Override
    public boolean executeStructureQuery(String query) throws SQLException {

        Query exp = QueryBuilder.buildQuery(query);
        if (!exp.isCorrect(query)) throw new SQLException("OPS!!");

        if (exp.getId() == 0 || exp.getId() == 2)
            return dealWithDatabase(query, exp);

        else if (exp.getId() == 1 || exp.getId() == 3)
            return dealWithTable(query, exp);

        else
            return false;
    }


    @Override
    public Object[][] executeQuery(String query) throws SQLException {

        Query exp = QueryBuilder.buildQuery(query);

        if (activeDataBase == null)
            throw new SQLException("THERE IS NO AN ACTIVE DATABASE!!!");

        if (exp.isCorrect(query) && exp.getId() == 7){
            Object[][] result=activeDataBase.select((HashMap<String, Object>) extractData.SelectedProperties(query));
            myLogger.info("selection Query performed on table "+extractData.getTableName(query));
            return result;
        }

        else
            throw new SQLException("you cant perform the select query");
    }


    @Override
    public int executeUpdateQuery(String query) throws SQLException {

        Query exp = QueryBuilder.buildQuery(query);

        if (activeDataBase == null || !exp.isCorrect(query))
            throw new SQLException("THERE IS NO AN ACTIVE DATABASE!!!");

        if (exp.getId() == 4){
            int result=activeDataBase.editTable(extractData.getObjectsToInsert(query), extractData.getTableName(query));
            myLogger.info(result+" record is added to table "+extractData.getTableName(query));
            return result;
        }

        else if (exp.getId() == 5){
            int result=activeDataBase.delete((HashMap<String, Object>) extractData.DeleteProperties(query));
            myLogger.info(result+" records are deleted from table "+extractData.getTableName(query));
            return result;
        }


        else if (exp.getId() == 6){
            int result=activeDataBase.update((HashMap<String, Object>) extractData.UpadteProperties(query));;
            myLogger.info(result+" records are updated in table "+extractData.getTableName(query));
            return result;
        }

        else
            throw new SQLException("Query is incorrect !! ");
    }

    // Set column names for selection
    static void setSelectedColumnNames(String[] selectedColumnNames) {
        DataBaseGenerator.selectedColumnNames = selectedColumnNames;
    }

    // Return column names in select
    public static String[] getSelectedColumnNames() {
        return DataBaseGenerator.selectedColumnNames;
    }

    // Set types of selected columns
    public static void setSelectedColumnTypes(Integer[] selectedColumnTypes) {
        DataBaseGenerator.selectedColumnTypes = selectedColumnTypes;
    }

    // Return column types in select
    public static Integer[] getSelectedColumnTypes() {
        return DataBaseGenerator.selectedColumnTypes;
    }

    private boolean dealWithDatabase(String query, Query exp) throws SQLException
    {
        MyDataBase oldActiveDatabase = this.activeDataBase;
        String dataBaseName = extractData.getDatabaseName(query);
        boolean isFound = searchForThatName(dataBaseName);

        if (exp.getId() == 2)
        {
            if (!isFound) {
                myLogger.warning("trying to drop database "+dataBaseName+" which doesn't exist.");
                return false;
               // throw new SQLException("THE TABLE DOESNT EXIST !!!!");
            }
            else
            {
                TableFactory.delete(activeDataBase.getPath());
                dataBases.remove(activeDataBase);
                myLogger.info("database "+activeDataBase.getName()+" is droped.");
                activeDataBase = null;
                return true;
            }
        }
        else if (exp.getId() == 0) {
            try {
                if (!isFound) {
                    activeDataBase = new MyDataBase(dataBaseName, pathToDatabases);
                    dataBases.add(activeDataBase);
                    myLogger.info("Data base "+activeDataBase.getName() +" added.");
                    return true;
                }
                else{
                    myLogger.info("Data base " +dataBaseName +" found.");
                    return true;
                }

            }
            catch (SQLTimeoutException e) {
                this.activeDataBase = oldActiveDatabase;
            }
        }
        return false;
    }

    private boolean dealWithTable(String query, Query exp) throws SQLException {
        if (activeDataBase == null)
            throw new SQLException();

        String tableName = extractData.getTableName(query);
        if (exp.getId() == 3) {
            if (activeDataBase.removeTable(tableName)) {
                myLogger.info("table " + tableName + " removed from database " + activeDataBase.getName() + ".");
                return true;
            }
            myLogger.warning("table " + tableName + " failed to removed.");
            return false;
        }
        else{
            if (activeDataBase.addTable(extractData.getContentsOfTableQuery(query), tableName)) {
                myLogger.info("table " + tableName + " added to database " + activeDataBase.getName() + ".");
                return true;
            }
            myLogger.warning("table " + tableName + "failed to add.");
            return false;
        }
    }

    private boolean searchForThatName(String databaseName)
    {
        boolean flag = false;
        for (MyDataBase dataBase : dataBases) {
            if (dataBase.getName().equalsIgnoreCase(databaseName)) {
               flag = true;
               activeDataBase = dataBase;
               break;
            }
        }
        return flag;
    }


    public Logger getMyLogger() {
        return myLogger;
    }
}
