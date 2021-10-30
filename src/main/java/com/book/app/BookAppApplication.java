package com.book.app;

import com.book.app.author.Author;
import com.book.app.author.AuthorRepository;
import com.book.app.connection.DataStaxAstraProperties;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BookAppApplication {

    @Autowired
    private AuthorRepository authorRepository;

    @Value("${datadump.location.author}")
    private String authorDumpLocation;

    @Value("${datadump.location.works}")
    private String worksDumpLocation;

    public static void main(String[] args) {
        SpringApplication.run(BookAppApplication.class, args);
    }

    private void initAuthors() {
        Path path = Paths.get(authorDumpLocation);
        try (Stream<String> lines = Files.lines(path)) {
            lines.forEach(line -> {
                //Read and parse line
                String jsonString = line.substring(line.indexOf("{"));
                try {
                    //Construct Author Object
                    JSONObject jsonObject = new JSONObject(jsonString);
                    Author author = Author.builder()
                            .name(jsonObject.getString("name"))
                            .personalName("Personal Name")
                            .id(jsonObject.getString("key").replace("/authors/", ""))
                            .build();

                    //Persist using repository
                    authorRepository.save(author);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initWorks() {

    }

    @PostConstruct
    public void start() {
        initAuthors();
        initWorks();
        System.out.println(authorDumpLocation);
    }

    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astaProperties) {
        Path bundle = astaProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }
}
