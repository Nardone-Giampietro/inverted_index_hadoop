package it.unipi.hadoop;

import java.util.Objects;

public class Word_File {
    String word;
    String file;

    public Word_File(String word, String file) {
        this.word = word;
        this.file = file;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!(obj instanceof Word_File))
            return false;
        Word_File wf = (Word_File) obj;
        return word.equals(wf.word) && file.equals(wf.file);
    }

    public int hashCode() {
        return Objects.hash(word, file);
    }
}
