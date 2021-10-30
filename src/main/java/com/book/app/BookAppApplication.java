package com.book.app;

import com.book.app.author.Author;
import com.book.app.author.AuthorRepository;
import com.book.app.book.Book;
import com.book.app.book.BookRepository;
import com.book.app.connection.DataStaxAstraProperties;
import org.json.JSONArray;
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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BookAppApplication {

    @Autowired
    private AuthorRepository authorRepository;

    @Autowired
    private BookRepository bookRepository;

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
        Path path = Paths.get(worksDumpLocation);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
        try (Stream<String> lines = Files.lines(path)) {
            lines.forEach(line -> {
                //Read and parse line
                String jsonString = line.substring(line.indexOf("{"));
                try {
                    //Construct Author Object
                    JSONObject jsonObject = new JSONObject(jsonString);
                    //Covers
                    JSONArray coversJSONArr = jsonObject.optJSONArray("covers");
                    List<String> coverIds = new ArrayList<>();
                    if (coversJSONArr != null) {
                        for (int i = 0; i < coversJSONArr.length(); i++) {
                            coverIds.add(coversJSONArr.getString(i));
                        }
                    }
                    //Author information
                    JSONArray authorsJSONArr = jsonObject.optJSONArray("authors");
                    List<String> authorsIds = new ArrayList<>();
                    List<String> authorsNames = new ArrayList<>();
                    if (coversJSONArr != null) {
                        for (int i = 0; i < authorsJSONArr.length(); i++) {
                            String authorId = authorsJSONArr.getJSONObject(i).getJSONObject("author").getString("key")
                                    .replace("/authors/", "");
                            authorsIds.add(authorId);
                            authorsNames = authorsIds.stream()
                                    .map(id -> authorRepository.findById(id))
                                    .map(optionalAuthor -> {
                                        if (!optionalAuthor.isPresent()) return "Unknown Author";
                                        return optionalAuthor.get().getName();
                                    }).collect(Collectors.toList());
                        }
                    }
                    //Description
                    JSONObject descriptionObj = jsonObject.optJSONObject("created");
                    String description = "";
                    if(descriptionObj != null){
                        description = descriptionObj.getString("value");
                    }
                    //Date
                    JSONObject publishedObj = jsonObject.optJSONObject("created");
                    String dateStr = "";
                    if (publishedObj != null) {
                        dateStr = publishedObj.getString("value");
                    }

                    Book book = Book.builder()
                            .id(jsonObject.getString("key").replace("/works/", ""))
                            .name(jsonObject.optString("title"))
                            .description(description)
                            .coverIds(coverIds)
                            .authorNames(authorsNames)
                            .authorIds(authorsIds)
                            .publishedDate(LocalDate.parse(dateStr, dateTimeFormatter))
                            .build();

                    bookRepository.save(book);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @PostConstruct
    public void start() {
        //initAuthors();
        initWorks();
        System.out.println(authorDumpLocation);
    }

    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astaProperties) {
        Path bundle = astaProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }
}
