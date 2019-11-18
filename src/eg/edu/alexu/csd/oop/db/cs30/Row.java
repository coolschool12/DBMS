package eg.edu.alexu.csd.oop.db;

import java.util.HashMap;

public class Row {
    private HashMap<String,Object> map=new HashMap<>();

    public Row(String[] columnNames, String[] columnNames1, Object[] values) {

        for(int i=0;i<columnNames.length;i++){
            map.put(columnNames[i],null);
        }
        for(int i=0;i<columnNames1.length;i++){
            map.remove(columnNames1[i]);
            map.put(columnNames1[i],values[i]);
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
            map.remove(columnNames[i]);
            map.put(columnNames[i],values[i]);
        }
    }
    public Boolean Condition(String ColumnName,char Operator,Object value,Integer type){
        //System.out.println("In condition  " +this.map );
        Object val=map.get(ColumnName);
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
                //System.out.println(val+" "+value);
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
