package rs.iggy.user;

import java.math.BigInteger;

public record TokenInfo(String token, BigInteger expiry) {
}
