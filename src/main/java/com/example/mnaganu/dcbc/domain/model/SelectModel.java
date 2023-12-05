package com.example.mnaganu.dcbc.domain.model;

import java.util.List;

@lombok.Value
@lombok.Builder
public class SelectModel<E> {
  private final List<E> list;
  private final int offset;
  private final int limit;
  private final int total;

}
