package com.ym.gmall.dws.function;

import com.ym.gmall.dws.util.IkUtil;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Set;

public class KwSplit extends TableFunction<Row> {
    public static void main(String[] args) {
        Set<String> keywords = IkUtil.split("macbook m2 15寸 笔记本电脑");
    }
    public void eval(String kw) {
        if (kw == null) {
            return;
        }
        // "华为手机白色手机"
        Set<String> keywords = IkUtil.split(kw);
        for (String keyword : keywords) {
            collect(Row.of(keyword));
        }
    }
}
