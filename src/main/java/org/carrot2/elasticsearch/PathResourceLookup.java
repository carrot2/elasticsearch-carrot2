package org.carrot2.elasticsearch;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.carrot2.util.ResourceLookup;

public class PathResourceLookup implements ResourceLookup {
  private final List<Path> locations;

  public PathResourceLookup(List<Path> locations) {
    if (locations == null || locations.isEmpty()) {
      throw new RuntimeException("At least one resource location is required.");
    }
    this.locations = locations;
  }

  @Override
  public InputStream open(String resource) throws IOException {
    Path p = locate(resource);
    if (p == null) {
      throw new IOException(
          "Resource "
              + p
              + " not found relative to: "
              + locations.stream()
                  .map(path -> path.toAbsolutePath().toString())
                  .collect(Collectors.joining(", ")));
    }
    return new BufferedInputStream(Files.newInputStream(p));
  }

  @Override
  public boolean exists(String resource) {
    return locate(resource) != null;
  }

  @Override
  public String pathOf(String resource) {
    return "["
        + locations.stream()
            .map(path -> path.resolve(resource).toAbsolutePath().toString())
            .collect(Collectors.joining(" | "))
        + "]";
  }

  private Path locate(String resource) {
    for (Path base : locations) {
      Path p = base.resolve(resource);
      if (Files.exists(p)) {
        return p;
      }
    }
    return null;
  }
}
