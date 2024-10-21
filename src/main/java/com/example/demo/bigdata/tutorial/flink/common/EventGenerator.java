package com.example.demo.bigdata.tutorial.flink.common;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description:
 * @Author: Chenyang on 2024/10/21 11:08
 * @Version: 1.0
 */
public class EventGenerator {

    public static List<SourceEvent1> getEvent1List() {
        List<SourceEvent1> list = new ArrayList<>();
        list.add(new SourceEvent1(1, "Jean", 1000L));
        list.add(new SourceEvent1(2, "Sean", 3000L));
        list.add(new SourceEvent1(3, "Tom", 7000L));
        list.add(new SourceEvent1(4, "Jeremy", 2000L));
        list.add(new SourceEvent1(5, "Sean", 2000L));
        list.add(new SourceEvent1(6, "Joe", 1000L));
        list.add(new SourceEvent1(7, "Jean", 5000L));
        list.add(new SourceEvent1(8, "Mary", 6000L));
        list.add(new SourceEvent1(9, "Jeremy", 1000L));
        list.add(new SourceEvent1(10, "Jean", 1000L));
        list.add(new SourceEvent1(11, "Tom", 5000L));
        list.add(new SourceEvent1(12, "Mary", 9000L));
        list.add(new SourceEvent1(13, "Jeremy", 6000L));
        list.add(new SourceEvent1(14, "Joe", 3000L));
        return list;
    }

    public static List<SourceEvent2> getEvent2List() {
        List<SourceEvent2> list = new ArrayList<>();
        list.add(new SourceEvent2("Mary", "/home", 1500L));
        list.add(new SourceEvent2("Alice", "/home", 1000L));
        list.add(new SourceEvent2("Bob", "/home", 2000L));
        list.add(new SourceEvent2("Mary", "/prod", 3000L));
        list.add(new SourceEvent2("Bob", "/prod", 3300L));
        list.add(new SourceEvent2("Alice", "/home", 1000L));
        list.add(new SourceEvent2("Alice", "/home", 1000L));
        list.add(new SourceEvent2("Bob", "/cart", 3500L));
        list.add(new SourceEvent2("Bob", "/prod?id=2", 3200L));
        list.add(new SourceEvent2("Mary", "/cart", 3800L));
        list.add(new SourceEvent2("Mary", "/prod?id=3", 5200L));
        list.add(new SourceEvent2("Mary", "/prod?id=4", 4200L));
        return list;
    }

    public static List<FlinkEvent> getFlinkEventList() {
        List<FlinkEvent> list = new ArrayList<>();
        list.add(new FlinkEvent("1","Jeremy",27));
        list.add(new FlinkEvent("2","Sean",18));
        list.add(new FlinkEvent("3","Melissa",26));
        list.add(new FlinkEvent("4","Tom",30));
        return list;
    }
}
