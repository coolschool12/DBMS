package eg.edu.alexu.csd.oop.test.db;
import org.junit.Assert;
import org.junit.Test;

import eg.edu.alexu.csd.oop.db.Database;

public class IntegrationTest {

    public static Class<?> getSpecifications(){
        return Database.class;
    }
    
    @Test
    public void test() {
        Assert.assertNotNull("Failed to create DBMS implemenation",  (Database)eg.edu.alexu.csd.oop.test.TestRunner.getImplementationInstanceForInterface(Database.class));
    }

}
