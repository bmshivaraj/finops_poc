package com.microfocus.finops.poc;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoField;


@JsonIgnoreProperties(ignoreUnknown = true)
public class XAuthToken {
    public long getPasswordExpiredDate() {
        return passwordExpiredDate;
    }

    public void setPasswordExpiredDate(long passwordExpiredDate) {
        this.passwordExpiredDate = passwordExpiredDate;
    }

    long passwordExpiredDate;
    Token token;

    public Token getToken() {
        return token;
    }

    public void setToken(Token token) {
        this.token = token;
    }

    public boolean isTokenExpired() {
        Instant tokenExpireTime = Instant.parse(getToken().getExpires());
        Instant currentTime = Instant.now();

        return currentTime.isAfter(tokenExpireTime);
    }

    public long secondsRemainingForTokeExpiry() {
        Instant tokenExpireTime = Instant.parse(getToken().getExpires());
        Instant currentTime = Instant.now();
        //tokenExpireTime.minus( currentTime.).getLong(ChronoField.INSTANT_SECONDS);
        Duration duration = Duration.between(currentTime, tokenExpireTime);

        return duration.getSeconds();
    }
}
