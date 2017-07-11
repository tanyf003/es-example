package cn.tanyf.elasticsearch.aggregation;

import com.alibaba.fastjson.JSON;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 这个是为分组定义的一个模型
 * catIds通常为一对多的分类ID
 */
public class AggregationTestModel implements Serializable {
    private static final long serialVersionUID = 3174577828007649745L;
    /**
     * 随便编写的一些值，type属性只能取这里面的其中一个
     */
    private String[] types = new String[] { "type1", "type2", "type3", "type4", "type5", "type6", "type7", "type11",
            "type12", "type13", "type14", "type15", "type16", "type17" };
    //主ID
    private long id;
    //类型，为types之一
    private String type;
    /**
     * 所属分类，范围为1-50
     */
    private List<Integer> catIds;
    
    private int grade;
    private double price;

    public AggregationTestModel() {
        Random r = new Random();
        int n = Math.abs(r.nextInt());
        int index = n % 14;
        this.type = types[index];
        this.grade = r.nextInt(100);
        java.math.BigDecimal b = new java.math.BigDecimal(r.nextDouble()*100);  
        double myNum3 = b.setScale(2, java.math.BigDecimal.ROUND_HALF_UP).doubleValue();  
        this.price = myNum3;

        n = n % 50;
        catIds = new ArrayList<Integer>();
        catIds.add(n);
        int ys = n % 3;
        if (ys != 0) {
            for (int i = 1; i < ys + 1; i++) {
                catIds.add(n + i);
            }
        }
    }

    public static void main(String[] argv) {
        for (int i = 0; i < 10; i++) {
            AggregationTestModel f = new AggregationTestModel();
            System.out.println(JSON.toJSON(f));
        }
    }

    public int getGrade() {
        return grade;
    }

    public void setGrade(int grade) {
        this.grade = grade;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<Integer> getCatIds() {
        return catIds;
    }

    public void setCatIds(List<Integer> catIds) {
        this.catIds = catIds;
    }
}