package ria.lettuce.pojo;

import java.util.Objects;

/**
 * @author YaoXunYu
 * created on 04/09/2019
 */
public class Article {
    private long id;
    private String title;
    private String link;
    private long posterId;
    private long time;
    private int votes;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getLink() {
        return link;
    }

    public void setLink(String link) {
        this.link = link;
    }

    public long getPosterId() {
        return posterId;
    }

    public void setPosterId(long posterId) {
        this.posterId = posterId;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public int getVotes() {
        return votes;
    }

    public void setVotes(int votes) {
        this.votes = votes;
    }

    @Override
    public String toString() {
        return "Article{" +
          "id=" + id +
          ", title='" + title + '\'' +
          ", link='" + link + '\'' +
          ", posterId=" + posterId +
          ", time=" + time +
          ", votes=" + votes +
          '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Article article = (Article) o;
        return id == article.id &&
          posterId == article.posterId &&
          time == article.time &&
          votes == article.votes &&
          Objects.equals(title, article.title) &&
          Objects.equals(link, article.link);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, title, link, posterId, time, votes);
    }
}
