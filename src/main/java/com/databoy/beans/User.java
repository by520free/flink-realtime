package com.databoy.beans;

/**
 * @author by_xiaopeng_27
 * @version V1.0
 * @Package com.databoy.beans
 * @Description: TODO
 * @date 2020/12/13 22:29
 */
public class User {

    private int id;
    private String uid;
    private String os;
    private String city;
    private String gender;
    private int age;
    private String education;
    private String occupation;
    private int marriage;
    private String hobby;
    private String income;
    private String consume;
    private int hasCar;
    private int hasHouse;
    private int hasChild;

    /**
     *  CREATE TABLE IF NOT EXISTS "user"(
     *      "uid" VARCHAR NOT NULL PRIMARY KEY,
     *      "id" INTEGER,
     *      "os" VARCHAR,
     *      "city" VARCHAR,
     *      "gender" VARCHAR,
     *      "age" INTEGER,
     *      "education" VARCHAR,
     *      "occupation" VARCHAR,
     *      "marriage" INTEGER,
     *      "hobby" VARCHAR,
     *      "income" VARCHAR,
     *      "consume" VARCHAR,
     *      "hasCar" INTEGER,
     *      "hasHouse" INTEGER,
     *      "hasChild" INTEGER
     *  ) SALT_BUCKETS = 3;
     *
     */


    public User() {
    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getOs() {
        return os;
    }

    public void setOs(String os) {
        this.os = os;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getEducation() {
        return education;
    }

    public void setEducation(String education) {
        this.education = education;
    }

    public String getOccupation() {
        return occupation;
    }

    public void setOccupation(String occupation) {
        this.occupation = occupation;
    }

    public int getMarriage() {
        return marriage;
    }

    public void setMarriage(int marriage) {
        this.marriage = marriage;
    }

    public String getHobby() {
        return hobby;
    }

    public void setHobby(String hobby) {
        this.hobby = hobby;
    }

    public String getIncome() {
        return income;
    }

    public void setIncome(String income) {
        this.income = income;
    }

    public String getConsume() {
        return consume;
    }

    public void setConsume(String consume) {
        this.consume = consume;
    }

    public int getHasCar() {
        return hasCar;
    }

    public void setHasCar(int hasCar) {
        this.hasCar = hasCar;
    }

    public int getHasHouse() {
        return hasHouse;
    }

    public void setHasHouse(int hasHouse) {
        this.hasHouse = hasHouse;
    }

    public int getHasChild() {
        return hasChild;
    }

    public void setHasChild(int hasChild) {
        this.hasChild = hasChild;
    }
}
