package ria.lettuce.pojo;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * @author YaoXunYu
 * created on 04/24/2019
 */
public class City {
    @JsonIgnore
    private int locId;
    private String city;
    private String region;
    private String country;

    public int getLocId() {
        return locId;
    }

    public void setLocId(int locId) {
        this.locId = locId;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }
}
