package eg.edu.alexu.csd.oop.db.cs30;

import eg.edu.alexu.csd.oop.db.Database;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

public class DataBaseGenerator implements Database {

    private ArrayList<MyDataBase> dataBases;
    private MyDataBase activeDataBase;
    private DataChecker dataChecker;
    DataBaseGenerator()
    {
        dataChecker = new DataChecker();
        dataBases = new ArrayList<>();
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

        if (dataChecker.checkCreateDatabase(query) || dataChecker.checkDropDatabase(query))
            return dealWithDatabase(query);

        else if (dataChecker.checkCreateTable(query) || dataChecker.checkDropTable(query))
            return dealWithTable(query);

        else
            return false;
    }


    @Override
    public Object[][] executeQuery(String query) throws SQLException {

        if (dataChecker.checkSelect(query))
            return activeDataBase.select((HashMap<String, String>) dataChecker.SelectedProperties(query));
        else
            throw new SQLException("OPS!!");
    }


    @Override
    public int executeUpdateQuery(String query) throws SQLException {

        if (activeDataBase == null)
            throw new SQLException("THERE IS NO AN ACTIVE DATABASE!!!");

        if (dataChecker.checkInsert(query))
            return activeDataBase.editTable(dataChecker.getObjectsToInsert(query), query.split(" ")[2]);

        else if (dataChecker.checkDelete(query))
            return activeDataBase.delete((HashMap<String, String>) dataChecker.DeleteProperties(query));

        else if (dataChecker.checkUpdate(query))
            return activeDataBase.update((HashMap<String, String>) dataChecker.UpadteProperties(query));

        else
            throw new SQLException("OPS!!");
    }


    private boolean dealWithDatabase(String query) throws SQLException
    {
        boolean isFound = searchForThatName(query.split(" ")[2]);

        if (query.split(" ")[0].equalsIgnoreCase("DROP"))
        {
            if (!isFound) {
                throw new SQLException("THE TABLE DOESNT EXIST !!!!");
            }

            else
            {
                dataBases.remove(activeDataBase);
                activeDataBase = null;
                return true;
            }

        }
        else if (query.split(" ")[0].equalsIgnoreCase("CREATE"))
        {
            if (!isFound)
            {
                activeDataBase = new MyDataBase(query.split(" ")[2]);
                dataBases.add(activeDataBase);
                return true;
            }
            else
                return true;
        }
        return false;
    }

    private boolean dealWithTable(String query) throws SQLException {
        if (activeDataBase == null)
            throw new SQLException();

        if (query.split(" ")[0].equalsIgnoreCase("DROP"))
            return activeDataBase.removeTable(query.split(" ")[2]);

        else
            return activeDataBase.addTable(dataChecker.getContentsOfTableQuery(query), query.split(" ")[2]);

    }

    private boolean searchForThatName(String databaseName)
    {
        boolean flag = false;
        for (MyDataBase dataBase : dataBases) {
            if (dataBase.getName().equals(databaseName)) {
               flag = true;
               activeDataBase = dataBase;
               break;
            }
        }
        return flag;
    }



}
