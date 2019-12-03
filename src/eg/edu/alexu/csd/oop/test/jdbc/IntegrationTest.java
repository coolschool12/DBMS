package eg.edu.alexu.csd.oop.test.jdbc;
import java.sql.Driver;

import org.junit.Assert;
import org.junit.Test;

import eg.edu.alexu.csd.oop.TestRunner;


public class IntegrationTest {

    public static Class<?> getSpecifications(){
        return Driver.class;
    }
    
    @Test
    public void test() {
        Assert.assertNotNull("Failed to create Driver implemenation",  (Driver)TestRunner.getImplementationInstanceForInterface(Driver.class));
    }

}
