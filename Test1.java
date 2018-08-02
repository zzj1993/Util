package com.dianwoba.bigdata;

import com.dianwoba.bigdata.zeus.mapper.BaseTest;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author zhangzhijun
 * @create 2018/8/2
 */
public class Test1  {

    @Test
    public void test(){
        String str = "create EXTERNAL table if not exists [db_name.][table_name] (\n" +
                "[col_name]                    [data_type]              COMMENT ['col_comment'],\n" +
                " )\n" +
                "partitioned by (${ptString})\n" +
                "stored as orc tblproperties (\"orc.compress\"=\"SNAPPY\");";
    }

    /**
     * 根据键值对填充字符串，如("hello ${name}",{name:"xiaoming"})
     * 输出：
     * @param content
     * @param map
     * @return
     */
    public static String renderString(String content, Map<String, String> map){
        Set<Map.Entry<String, String>> sets = map.entrySet();
        for(Map.Entry<String, String> entry : sets) {
            String regex = "\\$\\{" + entry.getKey() + "\\}";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(content);
            content = matcher.replaceAll(entry.getValue());
        }
        return content;
    }
    @Test
    public void renderString() {
        /*String content = "hello ${name}, 1 2 3 4 5 ${six} 7, again ${name}. ";
        Map<String, String> map = new HashMap<>();
        map.put("name", "java");
        map.put("six", "6");
        content = renderString(content, map);
        System.out.println(content);*/
        String createStr = "create ${external} table if not exists ${db_name}.${table_name} (\n" +
                "${columnInfo}\n" +
                " )\n" +
                "partitioned by (${ptString})\n" +
                "stored as ${file_format} tblproperties (\"orc.compress\"=\"${SNAPPY}\");";
        String columnInfoBase = "${col_name}                    ${data_type}              COMMENT '${col_comment}',\n";
        String ptStringBase = "${col_name} ${data_type},";

        StringBuffer sb = new StringBuffer();
        String columnTemp;
        Map<String, String> columnInfoMap = new HashMap<>();
        columnInfoMap.put("col_name", "id");
        columnInfoMap.put("data_type", "int");
        columnInfoMap.put("col_comment", "自增");
        columnTemp = renderString(columnInfoBase, columnInfoMap);
        sb.append(columnTemp);

        columnInfoMap = new HashMap<>();
        columnInfoMap.put("col_name", "name");
        columnInfoMap.put("data_type", "string");
        columnInfoMap.put("col_comment", "名称");
        columnTemp = renderString(columnInfoBase, columnInfoMap);
        sb.append(columnTemp);

        String columnInfo = sb.substring(0, sb.length()-2);
//        System.out.println(columnInfo);

        StringBuffer part = new StringBuffer();
        String partTemp;
        Map<String, String> ptStringMap = new HashMap<>();
        ptStringMap.put("col_name", "pt");
        ptStringMap.put("data_type", "string");
        partTemp = renderString(ptStringBase, ptStringMap);
        part.append(partTemp);

        ptStringMap = new HashMap<>();
        ptStringMap.put("col_name", "hour");
        ptStringMap.put("data_type", "string");
        partTemp = renderString(ptStringBase, ptStringMap);
        part.append(partTemp);

        String ptString = part.substring(0, part.length()-1);
//        System.out.println(ptString);

        Map<String, String> createMap = new HashMap<>();
        createMap.put("external", "external");
        createMap.put("db_name", "ods");
        createMap.put("table_name", "shop");
        createMap.put("file_format", "ORC");
        createMap.put("SNAPPY", "SNAPPY");
        createMap.put("columnInfo", columnInfo);
        createMap.put("ptString", ptString);
        createStr = renderString(createStr, createMap);

        System.out.println(createStr);
    }

}