package com.ag.peopledb.repository;

import com.ag.peopledb.anotation.Id;
import com.ag.peopledb.anotation.MultiSQL;
import com.ag.peopledb.anotation.SQL;
import com.ag.peopledb.exeption.UnableToSaveException;
import com.ag.peopledb.model.CrudOperation;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

abstract class CRUDRepository<T> {

    protected Connection connection;

    public CRUDRepository(Connection connection) {
        this.connection = connection;
    }


    private String getSQLByAnnotation(CrudOperation operationType, Supplier<String> sqlGetter){

        Stream<SQL> multiSqlStream = Arrays.stream(this.getClass().getDeclaredMethods())
                .filter(m -> m.isAnnotationPresent(MultiSQL.class))
                .map(m -> m.getAnnotation(MultiSQL.class))
                .flatMap(msql -> Arrays.stream(msql.value()));

        Stream<SQL> sqlStream = Arrays.stream(this.getClass().getDeclaredMethods())
                .filter(m -> m.isAnnotationPresent(SQL.class))
                .map(m -> m.getAnnotation(SQL.class));

        return Stream.concat(multiSqlStream, sqlStream)
              .filter(annotation -> annotation.operationType().equals(operationType))
              .map(SQL::value)
              .findFirst().orElseGet(sqlGetter);
    };

    public T save(T entity) throws UnableToSaveException {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(getSQLByAnnotation(CrudOperation.SAVE, this::getSaveSQL), Statement.RETURN_GENERATED_KEYS);

            mapForSave(entity, preparedStatement);
            int update = preparedStatement.executeUpdate();
            ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
            while (generatedKeys.next()){
                long id = generatedKeys.getLong(1);
                setIdByAnnotation(id, entity);
                System.out.println(entity);
            }
            System.out.printf("Records affected: %d%n", update );
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Saving new person failed: " + entity);
        }
        return entity;
    }

    public Optional<T> findPersonById(Long id) {
        T entity = null;

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(getSQLByAnnotation(CrudOperation.FIND_BY_ID, this::getFindByIdSQL));
            preparedStatement.setLong(1, id);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                entity = extractEntityFromResultSet(resultSet);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Nothing has been found. Try again later");
        }
        return Optional.ofNullable(entity);
    }

    public List<T> findAll(){
        List<T> entities = new ArrayList<>();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(getSQLByAnnotation(CrudOperation.FIND_ALL, this::getFindAllSQL));
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()){
                entities.add(extractEntityFromResultSet(resultSet));
            }

        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Nothing has been found. Try again later");
        }
        return entities;
    };

    public long count(){
        long count = 0;
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(getSQLByAnnotation(CrudOperation.COUNT, this::getCountSQL));
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()){
                count = resultSet.getLong(1);
            }
            return count;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Nothing to count. Try again later");
        }
    }

    private void setIdByAnnotation(Long id, T entity){
        Arrays.stream(entity.getClass().getDeclaredFields())
                .filter(f->f.isAnnotationPresent(Id.class))
                .forEach(f -> {
                    f.setAccessible(true);
                    try {
                        f.set(entity, id);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException("Unable to set field value" + e);
                    };
            });
    };


    private Long getIdByAnnotation(T entity) {
        return Arrays.stream(entity.getClass().getDeclaredFields())
                .filter(f->f.isAnnotationPresent(Id.class))
                .map(f -> {
                    f.setAccessible(true);
                    Long id = null;
                    try {
                        id = (long)f.get(entity);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                    return id;
                })
                .findFirst().orElseThrow(() -> new RuntimeException("No Id annotated field found"));
    }

    public void delete(T entity) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(getSQLByAnnotation(CrudOperation.DELETE_ONE, this::getDeleteSQL));
            preparedStatement.setLong(1, getIdByAnnotation(entity));
            int result = preparedStatement.executeUpdate();
            System.out.println(result + " - Deleted entity");
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Deleting failed. Try again later");
        }
    }

    public void delete(T...entities){
        try {
            String ids = Arrays.stream(entities).map(this::getIdByAnnotation).map(String::valueOf).collect(joining(","));
            Statement statement = connection.createStatement();
            int deletedRecordsCount = statement.executeUpdate(getSQLByAnnotation(CrudOperation.DELETE_MANY, this::getDeleteInSQL).replace(":ids", ids));
            System.out.println(deletedRecordsCount + " === deletedRecordsCount");
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Multiple deleting failed. Try again later");
        }
    }

    public void update(T entity) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(getSQLByAnnotation(CrudOperation.UPDATE, this::getUpdateSQL));
            mapForUpdate(entity, preparedStatement);
            preparedStatement.setLong(5, getIdByAnnotation(entity));
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Updating failed. Try again later");
        }
    }




    abstract T extractEntityFromResultSet(ResultSet resultSet) throws SQLException;

    /**
     * Returns a string that represents a SQL needed to retrieve one entity
     * The SQL must contain one SQL parameter i.e. "?" that would bind to entity's ID
     */


    abstract void mapForSave(T entity, PreparedStatement preparedStatement) throws SQLException;

    abstract void mapForUpdate(T entity, PreparedStatement preparedStatement) throws SQLException;

    protected String getDeleteInSQL(){ throw new RuntimeException("SQL Not defined.");};

    /**
     * should return a string like "DELETE FROM PEOPLE WHERE ID IN (:ids)"
     */
    protected String getDeleteSQL(){ throw new RuntimeException("SQL Not defined.");};

    protected String getCountSQL(){ throw new RuntimeException("SQL Not defined.");};

    protected String getFindAllSQL(){ throw new RuntimeException("SQL Not defined.");};

    protected String getFindByIdSQL(){ return "";};


    protected String getSaveSQL(){
          return "";
      };

    protected String getUpdateSQL(){
        return "";
    };



}
