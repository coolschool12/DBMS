package eg.edu.alexu.csd.oop.db.cs30;

import java.util.HashMap;

public class Row {
    private HashMap<String,Object> map=new HashMap<>();

    /**
     *
     * @param columnNames columns names you want to specify
     * @param values the values of (columnNames1)
     */
    public Row(String[] columnNames, Object[] values) {

        for(int i=0;i<columnNames.length;i++){
            map.put(columnNames[i],values[i]);
        }
    }
    public Object[] getRow(String[] columnNames){
        Object[] result=new Object[columnNames.length];
        for(int i=0;i<columnNames.length;i++){
            result[i]=map.get(columnNames[i]);
        }
        return result;
    }
    public void updateRow(String[] columnNames, Object[] values){
        for(int i=0;i<columnNames.length;i++){
            map.put(columnNames[i],values[i]);
        }
    }
    public Boolean Condition(String ColumnName,char Operator,Object value,Integer type){
        Object val=map.get(ColumnName);
        if(val == null)
            return false;
        if(type==1){
            if(Operator=='='){
                if(  ((Integer)val)  ==  ((Integer)value)  ) return true;
                else return false;
            }else if(Operator=='>'){
                if(  ((Integer)val)  >  ((Integer)value)  ) return true;
                else return false;
            }else if(Operator=='<'){
                if(  ((Integer)val)  <  ((Integer)value)  ) return true;
                else return false;
            }else{
                System.out.println("Not Supported Operator !");
                return false;
            }
        }else if(type==0){
            if(Operator=='='){
                if(  ((String)val).equals((String) value)  ) return true;
                else return false;
            }else if(Operator=='>'){
                if(  (  (String)val  ).compareTo(  ((String)value)  )  > 0  ) return true;
                else return false;
            }else if(Operator=='<'){
                if(  (  (String)val  ).compareTo(  ((String)value)  )  < 0  ) return true;
                else return false;
            }else{
                System.out.println("Not Supported Operator !");
                return false;
            }
        }else{
            System.out.println("Invalid type not 0 or 1");
            return false;
        }
    }
}
