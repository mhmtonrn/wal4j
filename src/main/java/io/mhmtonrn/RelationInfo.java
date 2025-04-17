package io.mhmtonrn;

import java.util.List;

record RelationInfo(int id, String namespace, String name, List<ColumnInfo> columns) {}
