/*
 * This file is part of Avocado, licensed under the MIT License (MIT).
 *
 * Copyright (c) 2016, Lexteam <http://www.lexteam.xyz/>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package xyz.lexteam.avocado.orm;

import com.google.common.collect.Lists;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Represents a MySQL database.
 */
public class Database {

    private final Connection connection;

    public Database(Connection connection) {
        this.connection = connection;
    }

    /**
     * Makes a table from the given class.
     *
     * @param tableClass The class containing the table.
     * @returns {@code True} if the table didn't exists and it was successfully created, else {@code false}.
     */
    public boolean makeTable(Class<?> tableClass) {
        if (!tableClass.isAnnotationPresent(Table.class)) {
            throw new UnsupportedOperationException("you need some milk you moron");
        }
        Table table = tableClass.getAnnotation(Table.class);

        if (this.doesTableExist(table.name())) {
            return false;
        }

        List<ColumnModel> columns = getColumnsInClass(tableClass);

        try {
            this.createTableStatement(table, columns).execute();
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    /**
     * Gets the table from MySQL.
     *
     * @param tableClass The class containing the table.
     * @param key The column name.
     * @param value The value of the column, your're looking for.
     * @param <T> The type of the class.
     * @return A constructed object of tableClass.
     */
    public <T> Optional<T> getRowWhere(Class<T> tableClass, String key, String value) {
        if (!tableClass.isAnnotationPresent(Table.class)) {
            throw new UnsupportedOperationException("you need some milk you moron");
        }
        Table table = tableClass.getAnnotation(Table.class);

        if (!this.doesTableExist(table.name())) {
            return Optional.empty();
        }

        List<ColumnModel> columns = getColumnsInClass(tableClass);

        try {
            T tableObject = tableClass.newInstance();

            return Optional.of(tableObject);
        } catch (InstantiationException e) {
            e.printStackTrace();
            return Optional.empty();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }

    /**
     * Gets all the rows in a table.
     *
     * @param tableClass The class containing the table.
     * @param <T> The type of the tableClass.
     * @return All of the rows in that table.
     */
    public <T> Optional<Set<T>> getRows(Class<T> tableClass) {
        if (!tableClass.isAnnotationPresent(Table.class)) {
            throw new UnsupportedOperationException("you need some milk you moron");
        }
        Table table = tableClass.getAnnotation(Table.class);

        if (!this.doesTableExist(table.name())) {
            return Optional.empty();
        }

        List<ColumnModel> columns = getColumnsInClass(tableClass);

        return Optional.empty();
    }

    /**
     * Adds the row to the table.
     *
     * @param tableObject The class containing the row.
     * @returns {@code True} if the table exists and it successfully inserted the row, else {@code false}.
     */
    public boolean addRow(Object tableObject) {
        if (!tableObject.getClass().isAnnotationPresent(Table.class)) {
            throw new UnsupportedOperationException("you need some milk you moron");
        }
        Table table = tableObject.getClass().getAnnotation(Table.class);

        if (!this.doesTableExist(table.name())) {
            return false;
        }

        List<ColumnModel> columns = getColumnsInClass(tableObject.getClass());

        try {
            this.insertIntoStatement(table, tableObject, columns).execute();
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    /**
     * @see Connection#close()
     */
    public void close() throws SQLException {
        this.connection.close();
    }

    private PreparedStatement createTableStatement(Table table, List<ColumnModel> columns) throws SQLException {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("CREATE TABLE %s ( ", table.name()));

        for (int i = 0; i < columns.size(); i++) {
            ColumnModel columnModel = columns.get(i);
            builder.append(String.format("%s %s",
                    columnModel.getColumn().name(), columnModel.getColumn().type().name()));
            if (i != (columns.size() - 1)) {
                builder.append(",");
            }
            builder.append(" ");
        }

        builder.append(");");
        return this.connection.prepareStatement(builder.toString());
    }

    private PreparedStatement insertIntoStatement(Table table, Object tableObject, List<ColumnModel> columns) throws
            SQLException {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("INSERT INTO %s (", table.name()));

        for (int i = 0; i < columns.size(); i++) {
            ColumnModel columnModel = columns.get(i);
            builder.append(columnModel.getColumn().name());
            if (i != (columns.size() - 1)) {
                builder.append(", ");
            }
        }
        builder.append(") VALUES (");

        for (int i = 0; i < columns.size(); i++) {
            ColumnModel columnModel = columns.get(i);

            Optional<Object> objectOptional = columnModel.getValue(tableObject);
            if (objectOptional.isPresent()) {
                builder.append(String.format("'%s'", objectOptional.get()));
                if (i != (columns.size() - 1)) {
                    builder.append(", ");
                }
            }
        }

        builder.append(");");
        return this.connection.prepareStatement(builder.toString());
    }

    private static List<ColumnModel> getColumnsInClass(Class<?> tableClass) {
        List<ColumnModel> columns = Lists.newArrayList();

        for (Field field : tableClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(Column.class)) {
                columns.add(new ColumnModel(field.getAnnotation(Column.class), field));
            }
        }

        return columns;
    }

    private boolean doesTableExist(String name) {
        // thanks to http://stackoverflow.com/questions/8829102/mysql-check-if-table-exists-without-using-select-from
        // TODO: apparently I can check properly using the information schema

        try {
            this.connection.prepareStatement(String.format("SELECT 1 FROM %s LIMIT 1;", name));
        } catch (SQLException e) {
            return false;
        }

        return true;
    }

    public static class ColumnModel {

        private final Column column;
        private final Field field;

        public ColumnModel(Column column, Field field) {
            this.column = column;
            this.field = field;
        }

        public Column getColumn() {
            return this.column;
        }

        public Field getField() {
            return this.field;
        }

        public Optional<Object> getValue(Object table) {
            try {
                this.field.setAccessible(true);
                return Optional.of(this.field.get(table));
            } catch (IllegalAccessException e) {
                e.printStackTrace();
                return Optional.empty();
            }
        }

        public void setValue(Object table, Object value) {
            try {
                this.field.setAccessible(true);
                this.field.set(table, value);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }
}
