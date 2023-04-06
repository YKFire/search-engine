package com.searchengine.springboot.tmp;

import java.util.HashMap;
import java.util.Map;

public class recordTest {
    public static void main(String[] args) {
        String keyword = "欧美连衣裙";
        String[] split = keyword.split("\\s+");
        for (String s : split) {
            System.out.println(s);
        }


        Map<String,Integer> map = new HashMap<>();
        map.put("a",1);
        map.put("b",2);
        map.put("c",3);
        map.put("d",4);
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            System.out.println(entry.getValue());
            System.out.println(entry.getKey());
        }
    }
}
