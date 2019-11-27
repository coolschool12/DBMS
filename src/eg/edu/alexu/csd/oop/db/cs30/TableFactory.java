package eg.edu.alexu.csd.oop.db.cs30;

import org.w3c.dom.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

public class TableFactory {
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
    private static Object[][] parser(ArrayList<String> coulmnNames, ArrayList<String> values, HashMap<String,Integer> map){
        return null;
    }
}

