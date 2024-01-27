package com.ag.peopledb.model;

import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class PersonTest {
    @Test
    public void testForPeopleEquality(){
        Person person1 = new Person("bob", "one", ZonedDateTime.of(200, 01, 01, 01, 01, 01, 01, ZoneId.of("-1")));
        Person person2 = new Person("bob", "one", ZonedDateTime.of(200, 01, 01, 01, 01, 01, 01, ZoneId.of("-1")));
        assertThat(person1).isEqualTo(person2);
    }

    @Test
    public void testForPeopleInequality(){
        Person person1 = new Person("bob", "one", ZonedDateTime.of(200, 01, 01, 01, 01, 01, 01, ZoneId.of("-1")));
        Person person2 = new Person("bob", "two", ZonedDateTime.of(200, 01, 01, 01, 01, 01, 01, ZoneId.of("-1")));
        assertThat(person1).isNotEqualTo(person2);
    }

}