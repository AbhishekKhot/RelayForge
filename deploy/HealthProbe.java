import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public final class HealthProbe {
    private HealthProbe() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1 || args.length > 2) {
            System.exit(2);
        }
        int expectedStatus = args.length == 2 ? Integer.parseInt(args[1]) : 200;
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        HttpRequest request = HttpRequest.newBuilder(URI.create(args[0]))
                .timeout(Duration.ofSeconds(7))
                .GET()
                .build();
        int status = client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
        if (status != expectedStatus) {
            System.exit(1);
        }
    }
}
