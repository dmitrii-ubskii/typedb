/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package hypergraph.graph;

import hypergraph.graph.util.AttributeSync;
import hypergraph.graph.util.IID;
import hypergraph.graph.util.Schema;
import hypergraph.graph.util.Storage;
import hypergraph.graph.vertex.ThingVertex;
import hypergraph.graph.vertex.TypeVertex;
import hypergraph.graph.vertex.Vertex;
import hypergraph.graph.vertex.impl.ThingVertexImpl;

import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static hypergraph.graph.util.IID.Vertex.Thing.generate;

public class ThingGraph implements Graph<IID.Vertex.Thing, ThingVertex> {

    private final Graphs graphManager;
    private final ConcurrentMap<IID.Vertex.Thing, ThingVertex> thingByIID;

    ThingGraph(Graphs graphManager) {
        this.graphManager = graphManager;
        thingByIID = new ConcurrentHashMap<>();
    }

    @Override
    public Storage storage() {
        return null;
    }

    @Override
    public ThingVertex get(IID.Vertex.Thing iid) {
        return null; // TODO
    }

    public <VALUE> ThingVertex.Attribute<VALUE> getAttribute(IID.Vertex.Attribute<VALUE> attributeIID) {
        return null;
    }

    @Override
    public void delete(ThingVertex vertex) {
        // TODO
    }

    public void commit() {
        thingByIID.values().parallelStream().filter(v -> !v.isInferred() && !v.schema().equals(Schema.Vertex.Thing.ATTRIBUTE)).forEach(
                vertex -> vertex.iid(generate(graphManager.storage().keyGenerator(), vertex.schema(), vertex.typeVertex().iid()))
        ); // thingByIID no longer contains valid mapping from IID to TypeVertex
        thingByIID.values().parallelStream().filter(v -> !v.isInferred()).forEach(Vertex::commit);
        clear(); // we now flush the indexes after commit, and we do not expect this Graph.Thing to be used again
    }

    @Override
    public void clear() {
        thingByIID.clear();
    }

    public ThingVertex create(Schema.Vertex.Thing schema, IID.Vertex.Type type, boolean isInferred) {
        IID.Vertex.Thing iid = generate(graphManager.keyGenerator(), schema, type);
        ThingVertex vertex = new ThingVertexImpl.Buffered(this, iid, isInferred);
        thingByIID.put(iid, vertex);
        return vertex;
    }

    public ThingVertex.Attribute<Boolean> putAttribute(TypeVertex type, boolean value, boolean isInferred) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Boolean.class);

        IID.Vertex.Attribute<Boolean> attIID = new IID.Vertex.Attribute.Boolean(type.iid(), value);
        return putAttribute(attIID, isInferred);
    }

    public ThingVertex.Attribute<Long> putAttribute(TypeVertex type, long value, boolean isInferred) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Long.class);

        IID.Vertex.Attribute<Long> attIID = new IID.Vertex.Attribute.Long(type.iid(), value);
        return putAttribute(attIID, isInferred);
    }

    public ThingVertex.Attribute<Double> putAttribute(TypeVertex type, double value, boolean isInferred) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(Double.class);

        IID.Vertex.Attribute<Double> attIID = new IID.Vertex.Attribute.Double(type.iid(), value);
        return putAttribute(attIID, isInferred);
    }

    public ThingVertex.Attribute<String> putAttribute(TypeVertex type, String value, boolean isInferred) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(String.class);
        assert value.length() == Schema.STRING_MAX_LENGTH;

        IID.Vertex.Attribute<String> attIID = new IID.Vertex.Attribute.String(type.iid(), value);
        return putAttribute(attIID, isInferred);
    }

    public ThingVertex.Attribute<LocalDateTime> putAttribute(TypeVertex type, LocalDateTime value, boolean isInferred) {
        assert type.schema().equals(Schema.Vertex.Type.ATTRIBUTE_TYPE);
        assert type.valueType().valueClass().equals(LocalDateTime.class);

        IID.Vertex.Attribute<LocalDateTime> attIID = new IID.Vertex.Attribute.DateTime(type.iid(), value);
        return putAttribute(attIID, isInferred);
    }

    public <VALUE> ThingVertex.Attribute<VALUE> putAttribute(IID.Vertex.Attribute<VALUE> attributeIID, boolean isInferred) {
        ThingVertex.Attribute<VALUE> vertex = getAttribute(attributeIID);

        if (vertex != null) {
            graphManager.storage().attributeSync().remove(attributeIID);
        } else {
            AttributeSync.CommitSync commitSync = graphManager.storage().attributeSync().get(attributeIID);
            vertex = new ThingVertexImpl.Buffered.Attribute<>(this, attributeIID, isInferred, commitSync);
            thingByIID.put(attributeIID, vertex);
        }

        return vertex;
    }

    public TypeGraph typeGraph() {
        return graphManager.type();
    }
}
