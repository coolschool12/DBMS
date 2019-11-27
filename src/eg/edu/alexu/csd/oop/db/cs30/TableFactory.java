package eg.edu.alexu.csd.oop.db.cs30;

import org.w3c.dom.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TableFactory {
    /**
     * Create table schema.
     */
    public static void createTableSchema(String path) {
    }

    /**
     * Load an xml table.
     */
    public static Table load(Table table,String pathName) throws SQLException{
        File xmlFile =new File(pathName);
        DocumentBuilderFactory factory= DocumentBuilderFactory.newInstance();
        DocumentBuilder builder;
        Document doc;
        try{
            builder=factory.newDocumentBuilder();
            doc=builder.parse(xmlFile);
        }catch (Exception e){
            throw new SQLException("failed to load file");
        }
        doc.getDocumentElement().normalize();
        Element root=doc.getDocumentElement();
        NodeList rows=root.getChildNodes();
        for(int i = 0 ;  i < rows.getLength()  ; i++) if (rows.item(i).getNodeType() == Node.ELEMENT_NODE){
            ArrayList<String> coulmnNames=new ArrayList<>();
            ArrayList<String> values=new ArrayList<>();
            Element row= (Element) rows.item(i);
            NodeList tags=row.getChildNodes();
            for(int j = 0; j < tags.getLength();  j++) if (tags.item(j).getNodeType() == Node.ELEMENT_NODE){
                Element tag= (Element) tags.item(j);
                coulmnNames.add(tag.getTagName());
                values.add(tag.getTextContent());
            }
            Object[][] result=parser(coulmnNames,values,table.getMap());
            table.insertRow(  (String[]) result[0] ,  result[1]  );
        }
        return table;
    }

    /**
     * @return Table containing data taken from a schema file.
     */
    public static Table loadSchema(String path) throws SQLException {
        Table table;
        List<String> columnNames = new ArrayList<>();
        List<Integer> columnTypes = new ArrayList<>();

        File file = new File(path);

        try {
            DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
            Document xmlDocument = documentBuilder.parse(file);

            // Find all elements
            NodeList elements = xmlDocument.getElementsByTagName("element");
            for (int i = 0; i < elements.getLength(); i++)
            {
                if (elements.item(i).getNodeType() == Node.ELEMENT_NODE)
                {
                    Element element = (Element) elements.item(i);
                    columnNames.add(element.getAttribute("name"));

                    String type = element.getAttribute("type");
                    if (type.equalsIgnoreCase("string"))
                    {
                        columnTypes.add(0);
                    }
                    else if (type.equalsIgnoreCase("integer"))
                    {
                        columnTypes.add(1);
                    }
                    else
                    {
                        throw new SQLException();
                    }
                }
            }
        }
        catch (Exception e) {
            throw new SQLException();
        }

        // Get table name
        String[] splitPath = path.replaceAll(".xsd", "").split("/");

        table = new Table(columnNames.toArray(new String[0]), columnTypes.toArray(new Integer[0]));
        table.setTableName(splitPath[splitPath.length - 1]);

        return table;
    }

    /**
     * Parse data taken from DOM object.
     * @return a 2d object array
     *      0: column names.
     *      1: values of objects.
     */
    public static Object[][] parser(List<String> columnNames, List<String> values, Map<String, Integer> columnType) throws SQLException {
        Object[] objectValues = new Object[columnNames.size()];

        // Parse data
        for (int i = 0, columns = columnNames.size(); i < columns; i++)
        {
            // Integer
            if (columnType.get(columnNames.get(i)).equals(1))
            {
                try {
                    objectValues[i] = Integer.parseInt(values.get(i));
                }
                catch (Exception e) {
                    throw new SQLException();
                }
            }
            // String
            else
            {
                objectValues[i] = values.get(i);
            }
        }

        Object[][] parsedValues = new Object[2][];
        parsedValues[0] = columnNames.toArray(new String[0]);
        parsedValues[1] = objectValues;

        return parsedValues;
    }
}


