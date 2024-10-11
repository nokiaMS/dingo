/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.example;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StudyTest {
    static StudyTest INSTANCE = new StudyTest();

    /**
     * stream相关:
     *      1. 独立元素生成stream.
     *      2. 数组生成stream.
     *      3. list生成stream。
     * stream的几个步骤：
     *      1. 中间操作（intermediate）
     *              filter  过滤操作：根据lambda函数进行过滤，满足条件的元素继续处理，不满足条件的元素不做后续处理。
     *              map     映射操作：把输入使用lambda函数进行处理，返回处理后的输出结果。
     *              limit   限制元素数量为n。
     *              distinct    结果去重。
     *              skip    跳过前面n个元素。
     *              peek    在处理stream的过程中查看每个元素，通常用于调试，日志记录和收集统计信息，而不会改变流的内容。
     *      2. 终结操作（terminate）
     *      3. 短路操作（short-circuiting）
     *  链接：
     *      https://blog.csdn.net/justloveyou_/article/details/79562574
     */
    public void func1() {
        //独立元素的stream。
        Stream<String> s = Stream.of("a", "b", "c");
        List<String> sList = s.filter($ -> !$.equals("a")).collect(Collectors.toList());
        System.out.println(sList);

        //数组的stream.
        String[] strArr = new String[] {"a", "b", "c"};
        Stream<String> s1 = Stream.of(strArr);      //数组第一种生成stream的方式。
        List<String> str1 = s1
            .map(String::toUpperCase)
            .collect(Collectors.toList());
        System.out.println(str1);

        Stream<String> s2 = Arrays.stream(strArr);  //数组第二种生成stream的方式。
        List<String> str2 = s2
            .map(String::toUpperCase)               //此种lambda的使用方式是最简洁的。
            .collect(Collectors.toList());
        System.out.println(str2);

        Stream<String> s3 = Arrays.stream(strArr);
        List<String> str3 = s3
            .map($ -> { return $.toUpperCase(); })  //此种lambda函数也是可以的，但是不是最简单的，s2的方式是最简单的lambda表达式。
            .collect(Collectors.toList());
        System.out.println(str3);

        //list使用stream。
        List<String> s4 = Arrays.asList("aa", "bb", "bb", "cc", "dd", "ee", "ff");
        List<String> str4 = s4.stream()             //list转换为stream的方式。
            .map(a -> { return a.toUpperCase(); })
            .skip(3)                                //跳过前面三个元素。
            .peek(item -> System.out.println("Current peek:" + item))       //查看中间结果。
            .distinct()                             //结果去重。
            .limit(5)                       //限制最终元素数量。
            .collect(Collectors.toList());
        System.out.println(str4);
    }

    public static void main(String[] args) {
        System.out.println("Hello.");

        StudyTest.INSTANCE.func1();
    }
}
