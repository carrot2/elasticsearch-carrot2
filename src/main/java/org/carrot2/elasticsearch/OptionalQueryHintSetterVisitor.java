
package org.carrot2.elasticsearch;

import java.util.Objects;
import org.carrot2.attrs.AcceptingVisitor;
import org.carrot2.attrs.AttrBoolean;
import org.carrot2.attrs.AttrDouble;
import org.carrot2.attrs.AttrEnum;
import org.carrot2.attrs.AttrInteger;
import org.carrot2.attrs.AttrObject;
import org.carrot2.attrs.AttrObjectArray;
import org.carrot2.attrs.AttrString;
import org.carrot2.attrs.AttrStringArray;
import org.carrot2.attrs.AttrVisitor;

final class OptionalQueryHintSetterVisitor implements AttrVisitor {
  private final String queryHint;

  OptionalQueryHintSetterVisitor(String queryHint) {
    this.queryHint = queryHint;
  }

  @Override
  public void visit(String key, AttrBoolean attr) {}

  @Override
  public void visit(String key, AttrInteger attr) {}

  @Override
  public void visit(String key, AttrDouble attr) {}

  @Override
  public void visit(String key, AttrString attr) {
    if (Objects.equals(key, "queryHint")) {
      attr.set(queryHint);
    }
  }

  @Override
  public void visit(String key, AttrStringArray attr) {}

  @Override
  public <T extends Enum<T>> void visit(String key, AttrEnum<T> attr) {}

  @Override
  public <T extends AcceptingVisitor> void visit(String key, AttrObject<T> attr) {}

  @Override
  public <T extends AcceptingVisitor> void visit(String key, AttrObjectArray<T> attr) {}
}
