package org.apache.iggy.flink.example.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

/**
 * Word count result for word count example.
 */
public class WordCount implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String word;
    private final long count;

    @JsonCreator
    public WordCount(
            @JsonProperty("word") String word,
            @JsonProperty("count") long count) {
        this.word = word;
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public long getCount() {
        return count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WordCount wordCount = (WordCount) o;
        return count == wordCount.count && Objects.equals(word, wordCount.word);
    }

    @Override
    public int hashCode() {
        return Objects.hash(word, count);
    }

    @Override
    public String toString() {
        return "WordCount{word='" + word + "', count=" + count + '}';
    }
}
