package rs.iggy.personalaccesstoken;

import java.math.BigInteger;
import java.util.Optional;

public record PersonalAccessTokenInfo(String name, Optional<BigInteger> expiryAt) {
}
