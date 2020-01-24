package org.carrot2.elasticsearch;

import org.carrot2.util.ResourceLookup;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public class PathResourceLookup implements ResourceLookup {
   private final Path base;

   public PathResourceLookup(Path base) {
      this.base = Objects.requireNonNull(base);
   }

   @Override
   public InputStream open(String resource) throws IOException {
      return new BufferedInputStream(Files.newInputStream(base.resolve(resource)));
   }

   @Override
   public boolean exists(String resource) {
      return Files.exists(base.resolve(resource));
   }

   @Override
   public String pathOf(String resource) {
      return base.resolve(resource).toAbsolutePath().normalize().toString();
   }
}
