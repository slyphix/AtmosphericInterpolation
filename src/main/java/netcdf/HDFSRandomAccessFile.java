package netcdf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import ucar.unidata.io.RandomAccessFile;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;


// taken from
// https://github.com/SciSpark/SciSpark/blob/master/src/main/java/org/dia/HDFSRandomAccessFile.java

public class HDFSRandomAccessFile extends RandomAccessFile {

    protected URI fileSystemURI;
    protected Path filePath;
    protected FSDataInputStream hfile;
    protected FileStatus fileStatus;

    public HDFSRandomAccessFile(URI fileSystemURI, Path path, Configuration conf) throws IOException {
        this(fileSystemURI, path, defaultBufferSize, conf);
    }

    public HDFSRandomAccessFile(URI fileSystemURI, Path path, int bufferSize, Configuration conf) throws IOException {
        super(bufferSize);
        this.fileSystemURI = fileSystemURI;
        filePath = path;
        this.location = path.toString();
        if (debugLeaks) {
            openFiles.add(location);
        }

        FileSystem fs = FileSystem.get(this.fileSystemURI, conf);
        hfile = fs.open(filePath);

        fileStatus = fs.getFileStatus(filePath);
    }

    @Override
    public void flush() {

    }

    @Override
    public synchronized void close() throws IOException {
        super.close();
        hfile.close();
    }

    public long getLastModified() {
        return fileStatus.getModificationTime();
    }

    @Override
    public long length() throws IOException {
        return fileStatus.getLen();
    }

    @Override
    protected int read_(long pos, byte[] b, int offset, int len) throws IOException {
        return hfile.read(pos, b, offset, len);
    }

    @Override
    public long readToByteChannel(WritableByteChannel dest, long offset, long nbytes) throws IOException {
        long need = nbytes;
        byte[] buf = new byte[4096];

        hfile.seek(offset);
        int count = 0;
        while (need > 0 && count != -1) {
            need -= count;
            dest.write(ByteBuffer.wrap(buf, 0, count));
            count = hfile.read(buf, 0, 4096);
        }
        return nbytes - need;
    }
}
