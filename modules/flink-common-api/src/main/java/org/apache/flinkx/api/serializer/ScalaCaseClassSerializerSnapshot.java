/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flinkx.api.serializer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** {@link TypeSerializerSnapshot} for {@link CaseClassSerializer}. */
@Internal
public final class ScalaCaseClassSerializerSnapshot<T extends scala.Product>
        extends CompositeTypeSerializerSnapshot<T, CaseClassSerializer<T>> {

    private static final int VERSION = 3;

    private Class<T> type;
    private boolean isCaseClassImmutable;

    /** Used via reflection. */
    @SuppressWarnings("unused")
    public ScalaCaseClassSerializerSnapshot() {
        super(CaseClassSerializer.class);
    }

    /** Used for the snapshot path. */
    public ScalaCaseClassSerializerSnapshot(CaseClassSerializer<T> serializerInstance) {
        super(serializerInstance);
        this.type = checkNotNull(serializerInstance.getTupleClass(), "tuple class can not be NULL");
        this.isCaseClassImmutable = serializerInstance.isCaseClassImmutable();
    }

    @Override
    protected int getCurrentOuterSnapshotVersion() {
        return VERSION;
    }

    @Override
    protected TypeSerializer<?>[] getNestedSerializers(
            CaseClassSerializer<T> outerSerializer) {
        return outerSerializer.getFieldSerializers();
    }

    @Override
    protected CaseClassSerializer<T> createOuterSerializerWithNestedSerializers(
            TypeSerializer<?>[] nestedSerializers) {
        checkState(type != null, "type can not be NULL");
        return new CaseClassSerializer<>(type, nestedSerializers, isCaseClassImmutable);
    }

    @Override
    protected void writeOuterSnapshot(DataOutputView out) throws IOException {
        checkState(type != null, "type can not be NULL");
        out.writeUTF(type.getName());
        out.writeBoolean(isCaseClassImmutable);
    }

    @Override
    protected void readOuterSnapshot(
            int readOuterSnapshotVersion, DataInputView in, ClassLoader userCodeClassLoader)
            throws IOException {
        this.type = InstantiationUtil.resolveClassByName(in, userCodeClassLoader);
        // If reading a version of 2 or below, don't read the boolean and set isCaseClassImmutable to false
        this.isCaseClassImmutable = readOuterSnapshotVersion > 2 && in.readBoolean();
    }

    @Override
    protected CompositeTypeSerializerSnapshot.OuterSchemaCompatibility
    resolveOuterSchemaCompatibility(CaseClassSerializer<T> newSerializer) {
        var currentTypeName = Optional.ofNullable(type).map(Class::getName);
        var newTypeName = Optional.ofNullable(newSerializer.getTupleClass()).map(Class::getName);
        if (currentTypeName.equals(newTypeName)) {
            return OuterSchemaCompatibility.COMPATIBLE_AS_IS;
        } else {
            return OuterSchemaCompatibility.INCOMPATIBLE;
        }
    }
}
