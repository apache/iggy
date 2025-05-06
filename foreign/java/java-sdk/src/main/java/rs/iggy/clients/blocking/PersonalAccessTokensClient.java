package rs.iggy.clients.blocking;

import rs.iggy.personalaccesstoken.PersonalAccessTokenInfo;
import rs.iggy.personalaccesstoken.RawPersonalAccessToken;
import rs.iggy.user.IdentityInfo;
import java.math.BigInteger;
import java.util.List;

public interface PersonalAccessTokensClient {

    RawPersonalAccessToken createPersonalAccessToken(String name, BigInteger expiry);

    List<PersonalAccessTokenInfo> getPersonalAccessTokens();

    void deletePersonalAccessToken(String name);

    IdentityInfo loginWithPersonalAccessToken(String token);

}
