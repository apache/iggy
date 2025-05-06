package rs.iggy.clients.blocking.http;

import com.fasterxml.jackson.core.type.TypeReference;
import rs.iggy.clients.blocking.PersonalAccessTokensClient;
import rs.iggy.personalaccesstoken.PersonalAccessTokenInfo;
import rs.iggy.personalaccesstoken.RawPersonalAccessToken;
import rs.iggy.user.IdentityInfo;
import rs.iggy.user.TokenInfo;
import java.math.BigInteger;
import java.util.List;

class PersonalAccessTokensHttpClient implements PersonalAccessTokensClient {

    private static final String PERSONAL_ACCESS_TOKENS = "/personal-access-tokens";
    private final InternalHttpClient httpClient;

    public PersonalAccessTokensHttpClient(InternalHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public RawPersonalAccessToken createPersonalAccessToken(String name, BigInteger expiry) {
        var request = httpClient.preparePostRequest(PERSONAL_ACCESS_TOKENS,
                new CreatePersonalAccessToken(name, expiry));
        return httpClient.execute(request, new TypeReference<>() {
        });
    }

    @Override
    public List<PersonalAccessTokenInfo> getPersonalAccessTokens() {
        var request = httpClient.prepareGetRequest(PERSONAL_ACCESS_TOKENS);
        return httpClient.execute(request, new TypeReference<>() {
        });
    }

    @Override
    public void deletePersonalAccessToken(String name) {
        var request = httpClient.prepareDeleteRequest(PERSONAL_ACCESS_TOKENS + "/" + name);
        httpClient.execute(request);
    }

    @Override
    public IdentityInfo loginWithPersonalAccessToken(String token) {
        var request = httpClient.preparePostRequest(PERSONAL_ACCESS_TOKENS + "/login",
                new LoginWithPersonalAccessToken(token));
        var response = httpClient.execute(request, IdentityInfo.class);
        httpClient.setToken(response.accessToken().map(TokenInfo::token));
        return response;
    }

    record CreatePersonalAccessToken(String name, BigInteger expiry) {
    }

    record LoginWithPersonalAccessToken(String token) {
    }

}
