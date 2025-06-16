package it.unipi.hadoop;

import java.util.Objects;

public class Word_File{
    String word;
    String file;

    public Word_File(String word, String file){
        this.word=word;
        this.file=file;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof Word_File))
            return false;
        Word_File wf = (Word_File) o;
        return word.equals(wf.word) && file.equals(wf.file);
    }

    public int hashCode() {
        return Objects.hash(word, file);
    }
}

