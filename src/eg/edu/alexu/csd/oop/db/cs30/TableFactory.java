package eg.edu.alexu.csd.oop.db.cs30;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TableFactory {

    public static void createTable(String databasePath, String tableName, String[] columnNames, Integer[] columnTypes) throws SQLException {
        createTableSchema(databasePath+System.getProperty("file.separator")+tableName+".xsd", columnNames, columnTypes);

        // Create empty file
        try {
            DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
            Document xmlDocument = documentBuilder.newDocument();

            Element element = xmlDocument.createElement("rows");
            xmlDocument.appendChild(element);

            writeToFile(databasePath+System.getProperty("file.separator")+tableName+".xml", xmlDocument);
        }
        catch (Exception e) {
            throw new SQLException();
        }
    }

    /**
     * Load a table from xml
     */
    public static Table loadTable(String tablePath,String schemaPath) throws SQLException {
        Table table = loadSchema(schemaPath);
        return load(table, tablePath);
    }



    public static void saveTable(String tablePath, Table table) throws SQLException{

        try{
            // load the Document Builder and return a document
            Document doc = loadDocument();

            // load all the data from the table and put it in nodes in the doc object
            saveCellstoXml(table, doc);

            // send all the stuff to the transformer so it can transform the doc object to the file with that path
            writeToFile(tablePath  + System.getProperty("file.separator") + table.getTableName() + ".xml", doc);

           // createTableSchema(tablePath  + System.getProperty("file.separator") + table.getTableName() + ".xsd",  table.getColumnNames(), table.getColumnTypes());

        } catch (ParserConfigurationException | TransformerException e) {

            throw new SQLException("CANT SAVE THAT FILE!!!!!!");
        }
    }

    public static boolean delete(String filePath) {

        File file = new File(filePath);

        if (file.isDirectory())
        {
            String[] files = file.list();
            if (files != null) {
                for (String url : files) {
                    File nestedFile = new File(file.getPath(), url);
                    nestedFile.delete();
                }
            }
        }
        return file.delete();
    }

    /**
     * Create database schema
     */
    public static void createDatabaseSchema(String[] tableNames, String databaseSchemaPath) throws SQLException {
        try {
            DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
            Document xmlDocument = documentBuilder.newDocument();

            Element root = xmlDocument.createElement("schema");
            xmlDocument.appendChild(root);

            for (String tableName : tableNames) {
                // Create element and attributes
                Element columnElement = xmlDocument.createElement("element");
                columnElement.setAttribute("name", tableName);
                root.appendChild(columnElement);
            }

            // Write to file
            writeToFile(databaseSchemaPath, xmlDocument);
        }
        catch (Exception e) {
            throw new SQLException();
        }
    }

    /**
     * Read database schema
     */
    public static ArrayList<String> readDatabaseSchema(String schemaPath) throws SQLException {
        File file = new File(schemaPath);

        ArrayList<String> tableNames = new ArrayList<>();

        if (!file.exists())
        {
            return tableNames;
        }

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
                    tableNames.add(element.getAttribute("name"));
                }
            }
        }
        catch (Exception e) {
            throw new SQLException("Error while loading schema");
        }

        return tableNames;
    }

    /**
     * Create table schema.
     */
    private static void createTableSchema(String schemaPath, String[] columnNames, Integer[] columnTypes) throws SQLException {
        try {
            DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
            Document xmlDocument = documentBuilder.newDocument();

            Element root = xmlDocument.createElement("schema");
            xmlDocument.appendChild(root);

            for (int i = 0, columns = columnNames.length; i < columns; i++)
            {
                // Create element and attributes
                Element columnElement = xmlDocument.createElement("element");
                columnElement.setAttribute("name", columnNames[i]);

                if (columnTypes[i] == 0)
                {
                    columnElement.setAttribute("type", "string");
                }
                else
                {
                    columnElement.setAttribute("type", "integer");
                }

                root.appendChild(columnElement);
            }

            // Write to file
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");

            DOMSource domSource = new DOMSource(xmlDocument);
            StreamResult streamResult = new StreamResult(new File(schemaPath));

            transformer.transform(domSource, streamResult);
        }
        catch (Exception e) {
            throw new SQLException("Error creating xsd schema");
        }
    }

    /**
     * Load an xml table.
     */
    private static Table load(Table table,String pathName) throws SQLException{
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
        for(int i = 0 ;  i < rows.getLength() && threadInterrupted()  ; i++) if (rows.item(i).getNodeType() == Node.ELEMENT_NODE){
            ArrayList<String> coulmnNames=new ArrayList<>();
            ArrayList<String> values=new ArrayList<>();
            Element row= (Element) rows.item(i);
            NodeList tags=row.getChildNodes();
            for(int j = 0; j < tags.getLength() && threadInterrupted();  j++) if (tags.item(j).getNodeType() == Node.ELEMENT_NODE){
                Element tag= (Element) tags.item(j);
                coulmnNames.add(tag.getTagName());
                values.add(tag.getTextContent());
            }
            Object[][] result=parser(coulmnNames,values,table.getMap());
            table.insertRow((String[]) result[0] ,  result[1]);
        }
        return table;
    }

    /**
     * @return Table containing data taken from a schema file.
     */
    private static Table loadSchema(String path) throws SQLException {
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
            for (int i = 0; i < elements.getLength() && threadInterrupted(); i++)
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
            throw new SQLException("Error while loading schema");
        }

        // Get table name
        table = new Table(columnNames.toArray(new String[0]), columnTypes.toArray(new Integer[0]));
        table.setTableName(file.getName().replaceAll(".xsd", ""));

        return table;
    }

    /**
     * Parse data taken from DOM object.
     * @return a 2d object array
     *      0: column names.
     *      1: values of objects.
     */
    private static Object[][] parser(List<String> columnNames, List<String> values, Map<String, Integer> columnType) throws SQLException {
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

    private static Document loadDocument() throws ParserConfigurationException {


        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();

        return documentBuilder.newDocument();
    }

    private static void saveCellstoXml(Table table, Document doc) throws SQLException {
        Element rootElement = doc.createElement("rows");
        doc.appendChild(rootElement);

        Object[][] valuesOfTheTable = table.select();
        String[] tableColumnNames = table.getColumnNames();

        for (Object[] objects : valuesOfTheTable) {
            Element row = doc.createElement("row");
            for (int i = 0; i < objects.length && threadInterrupted(); i++) {
                if (objects[i] != null) {

                    Element cell = doc.createElement(tableColumnNames[i]);
                    cell.appendChild(doc.createTextNode(objects[i].toString()));
                    row.appendChild(cell);
                }
            }
            rootElement.appendChild(row);
        }
    }

    private static void writeToFile(String xmlPath, Document document) throws TransformerException, SQLException {

        threadInterrupted();

        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");

        DOMSource domSource = new DOMSource(document);
        StreamResult streamResult = new StreamResult(new File(xmlPath));

        transformer.transform(domSource, streamResult);
    }

    public static boolean threadInterrupted () throws SQLException {
        if (Thread.interrupted())
            throw new SQLTimeoutException("Time EXCEEDED !! ");
        else
            return true;

    }


}


