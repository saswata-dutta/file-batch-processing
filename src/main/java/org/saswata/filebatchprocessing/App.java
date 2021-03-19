/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package org.saswata.filebatchprocessing;

import java.util.function.Consumer;

public class App {

    public static void main(String[] args) {

        Consumer<String[]> printLines = (String[] chunk) -> {
            for (String s : chunk) {
                System.out.println(s.substring(0, 2));
            }
            System.out.println(666 / 0);
        };

        FileBatchProcessor processor = new FileBatchProcessor(
                ".gitignore",
                10,
                printLines,
                2,
                5);
        processor.run();
    }
}