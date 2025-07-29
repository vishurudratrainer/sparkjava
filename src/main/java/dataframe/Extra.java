package dataframe;

public class Extra{
    private String link;
    private String wikipedia;

    @Override
    public String toString() {
        return "Link{" +
                "link='" + link + '\'' +
                ", wikipedia='" + wikipedia + '\'' +
                '}';
    }

    public String getLink() {
        return link;
    }

    public void setLink(String link) {
        this.link = link;
    }

    public String getWikipedia() {
        return wikipedia;
    }

    public void setWikipedia(String wikipedia) {
        this.wikipedia = wikipedia;
    }
}
