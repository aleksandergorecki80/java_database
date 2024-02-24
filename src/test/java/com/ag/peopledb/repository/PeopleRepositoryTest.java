package com.ag.peopledb.repository;

import com.ag.peopledb.model.Address;
import com.ag.peopledb.model.Person;
import com.ag.peopledb.model.Region;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;

public class PeopleRepositoryTest {

    private Connection connection;
    private PeopleRepository repo;

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:h2:C:/Users/justy/Desktop/JAVA/DB/peopledb;TRACE_LEVEL_SYSTEM_OUT=0");
        connection.setAutoCommit(false);
        repo = new PeopleRepository(connection);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if(connection != null){
            connection.close();
        }
    }

    @Test
    public void canSaveOnePerson() {
        Person john = new Person("John", "Connor", ZonedDateTime.of(1980, 11, 01, 21, 05, 10, 0, ZoneId.of("-7")));
        Person savedPerson = repo.save(john);
        assertThat(savedPerson.getId()).isGreaterThan(0);
    }

    @Test
    public void canSaveTwoPeople(){
        Person john = new Person("John", "Connor", ZonedDateTime.of(1980, 11, 01, 21, 05, 10, 0, ZoneId.of("-7")));
        Person bob = new Person("Bob", "Smith", ZonedDateTime.of(1985, 05, 04, 01, 05, 10, 0, ZoneId.of("+4")));
        Person savedPerson1 = repo.save(john);
        Person savedPerson2 = repo.save(bob);
        assertThat(savedPerson1.getId()).isNotEqualTo(savedPerson2.getId());
    }

    @Test
    public void canSavePersonWithHomeAddress() throws SQLException {
        Person john = new Person("Waldemar", "Pawlak", ZonedDateTime.of(1980, 11, 01, 21, 05, 10, 0, ZoneId.of("-7")));
        Address address = new Address(null,"123 Bale st", "Apt 1a", "Wala Wala", "WA", "90210", "United States", "Fulton country", Region.WEST);

        john.setHomeAddress(address);
        Person savedPerson = repo.save(john);
        assertThat(savedPerson.getHomeAddress().get().id()).isGreaterThan(0);

//        connection.commit();
    }


    @Test
    public void canSavePersonWithBusinessAddress() throws SQLException {
        Person john = new Person("Waldemar", "Pawlak", ZonedDateTime.of(1980, 11, 01, 21, 05, 10, 0, ZoneId.of("-7")));
        Address address = new Address(null,"123 Bale st", "Apt 1a", "Wala Wala", "WA", "90210", "United States", "Fulton country", Region.WEST);

        john.setBusinessAddress(address);
        Person savedPerson = repo.save(john);
        assertThat(savedPerson.getBusinessAddress().get().id()).isGreaterThan(0);

//        connection.commit();
    }

    @Test
    public void canFindPersonByIdWithHomeAddress() throws SQLException {
        Person john = new Person("Waldemar", "Pawlak", ZonedDateTime.of(1980, 11, 01, 21, 05, 10, 0, ZoneId.of("-7")));
        Address address = new Address(null,"123 Bale st", "Apt 1a", "Wala Wala", "WA", "90210", "United States", "Fulton country", Region.WEST);

        john.setHomeAddress(address);
        Person savedPerson = repo.save(john);

        Person foundPerson = repo.findById(savedPerson.getId()).get();

        assertThat(foundPerson.getHomeAddress().get().state()).isEqualTo("WA");

//        connection.commit();
    }

    @Test
    public void canFindPersonByIdWithBusinessAddress() throws SQLException {
        Person john = new Person("Waldemar", "Pawlak", ZonedDateTime.of(1980, 11, 01, 21, 05, 10, 0, ZoneId.of("-7")));
        Address address = new Address(null,"123 Bale st", "Apt 1a", "Wala Wala", "WA", "90210", "United States", "Fulton country", Region.WEST);

        john.setBusinessAddress(address);
        Person savedPerson = repo.save(john);

        Person foundPerson = repo.findById(savedPerson.getId()).get();

        assertThat(foundPerson.getBusinessAddress().get().state()).isEqualTo("WA");

//        connection.commit();
    }

    @Test
    public void canSavePersonWithChildren() throws SQLException {
        Person john = new Person("Waldemar", "Pawlak", ZonedDateTime.of(1980, 11, 01, 21, 05, 10, 0, ZoneId.of("-7")));
        john.addChild(new Person("Zenon", "Pawlak", ZonedDateTime.of(2001, 07, 11, 21, 05, 10, 0, ZoneId.of("-7"))));
        john.addChild(new Person("Kazimierz", "Pawlak", ZonedDateTime.of(2005, 07, 11, 21, 05, 10, 0, ZoneId.of("-7"))));
        john.addChild(new Person("Leon", "Pawlak", ZonedDateTime.of(2003, 07, 11, 21, 05, 10, 0, ZoneId.of("-7"))));

        Person savedPerson = repo.save(john);

        savedPerson.getChildren().stream()
                        .map(Person::getId)
                        .forEach(id -> assertThat(id).isGreaterThan(0));

        connection.commit();
    }

    @Test
    public void canFindPersonById(){
        Person savedPerson = repo.save(new Person("test", "ofSaving", ZonedDateTime.now()));
            Person foundPerson = repo.findById(savedPerson.getId()).get();
        assertThat(foundPerson).isEqualTo(savedPerson);
    }

    @Test
    public void canFindPersonByIdWithChildren(){
        Person john = new Person("Waldemar", "Pawlak", ZonedDateTime.of(1980, 11, 01, 21, 05, 10, 0, ZoneId.of("-7")));
        john.addChild(new Person("Zenon", "Pawlak", ZonedDateTime.of(2001, 07, 11, 21, 05, 10, 0, ZoneId.of("-7"))));
        john.addChild(new Person("Kazimierz", "Pawlak", ZonedDateTime.of(2005, 07, 11, 21, 05, 10, 0, ZoneId.of("-7"))));
        john.addChild(new Person("Leon", "Pawlak", ZonedDateTime.of(2003, 07, 11, 21, 05, 10, 0, ZoneId.of("-7"))));

        Person savedPerson = repo.save(john);

        Person foundPerson = repo.findById(savedPerson.getId()).get();
        assertThat(foundPerson.getChildren().stream().map(Person::getFirstName).collect(toSet()))
                .contains("Waldemar", "Zenon", "Kazimierz", "Leon");
    }

    @Test
    public void testPersonIdNotFound(){
        Optional<Person> personById = repo.findById(-1L);
        assertThat(personById).isEmpty();
    }

    @Test
    public void canGetCount(){
        long startCount = repo.count();
        repo.save(new Person("test", "ofSaving", ZonedDateTime.now()));
        repo.save(new Person("test", "ofSaving", ZonedDateTime.now()));
        long endCount = repo.count();

        assertThat(endCount).isEqualTo(startCount + 2);
    }

    @Test
    public void canDelete(){
        Person savedPerson = repo.save(new Person("test", "ofSaving", ZonedDateTime.now()));
        long startCount = repo.count();

        repo.delete(savedPerson);
        long endCount = repo.count();

        assertThat(startCount).isEqualTo(endCount + 1);
    }

    @Test
    public void canDeleteMultiplePeople(){
        Person person = repo.save(new Person("John", "Doe", ZonedDateTime.now()));
        Person person1 = repo.save(new Person("Jan", "Nowak", ZonedDateTime.now()));
        long startCount = repo.count();

        repo.delete(person, person1);
        long endCount = repo.count();
        assertThat(endCount).isEqualTo(startCount -2);
    }

    @Test
    public void canUpdate(){
        String salary = "74587.21";

        Person savedPerson = repo.save(new Person("John", "Doe", ZonedDateTime.now()));
        Person foundPerson = repo.findById(savedPerson.getId()).get();

        savedPerson.setSalary(new BigDecimal(salary));
        repo.update(savedPerson);

        Person updatedPerson = repo.findById(savedPerson.getId()).get();

//        assertThat(foundPerson.getSalary()).isEqualTo("0");
//        assertThat(updatedPerson.getSalary()).isEqualTo(salary);
        assertThat(updatedPerson.getSalary()).isNotEqualTo(foundPerson.getSalary());
    }

    @Test
    @Disabled
    public void loadData() throws IOException, SQLException {
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("M/d/yyyy");
        DateTimeFormatter timeFormatter = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .appendPattern("h:mm:ss a")
                .toFormatter(Locale.US);

        Files.lines(Path.of("C:/Users/justy/Desktop/JAVA/UDEMY__Java_Foundations/Hr5m.csv"))
                .skip(1)
                .limit(1)
                .map(l -> l.split( ","))
                .map(a -> {
                    LocalDate dob = LocalDate.parse(a[10], dateFormatter);
                    LocalTime tob = LocalTime.parse(a[11], timeFormatter);
                    LocalDateTime dtob = LocalDateTime.of(dob, tob);
                    ZonedDateTime zdtob = ZonedDateTime.of(dtob, ZoneId.of("+0"));
                    Person person = new Person(a[2], a[4], zdtob);
                    person.setSalary(new BigDecimal(a[25]));
                    person.setEmail(a[6]);
                    return person;
                })
                .forEach(repo::save);
        connection.commit();
    }

    @Test
    @Disabled
    public void canFindAll(){
        long count = repo.count();
        List<Person> all = repo.findAll();
        assertThat(all.size()).isEqualTo(count);
    }

}
