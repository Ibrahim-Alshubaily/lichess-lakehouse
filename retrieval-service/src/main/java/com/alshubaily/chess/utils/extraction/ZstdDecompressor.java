package com.alshubaily.chess.utils.extraction;

import com.github.luben.zstd.ZstdInputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class ZstdDecompressor {
    public static InputStream decompress(File input) throws IOException {
        FileInputStream fis = new FileInputStream(input);
        return new ZstdInputStream(fis);
    }
}

