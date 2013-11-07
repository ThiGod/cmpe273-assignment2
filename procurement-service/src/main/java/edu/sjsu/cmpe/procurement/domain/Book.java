package edu.sjsu.cmpe.procurement.domain;

import java.net.URL;


public class Book {
    private long isbn;
    private String title;

    // add more fields here
    private String category;
    private URL coverimage;
    
    /**
     * @return the isbn
     */
    public long getIsbn() {
	return isbn;
    }

    /**
     * @param isbn
     *            the isbn to set
     */
    public void setIsbn(long isbn) {
	this.isbn = isbn;
    }

    /**
     * @return the title
     */
    public String getTitle() {
	return title;
    }

    /**
     * @param title
     *            the title to set
     */
    public void setTitle(String title) {
	this.title = title;
    }
    
    public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public URL getCoverimage() {
		return coverimage;
	}

	public void setCoverimage(URL coverimage) {
		this.coverimage = coverimage;
	}
}
