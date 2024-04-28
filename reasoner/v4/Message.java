package com.vaticle.typedb.core.reasoner.v4;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.Map;
import java.util.Objects;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_CAST;

public abstract class Message {
    private final MessageType msgType;
    private final int index;

    public Message.Answer asAnswer() {
        throw TypeDBException.of(ILLEGAL_CAST, this.getClass(), Answer.class);
    }

    public Message.Conclusion asConclusion() {
        throw TypeDBException.of(ILLEGAL_CAST, this.getClass(), Conclusion.class);
    }

    public HitInversion asHitInversion() {
        throw TypeDBException.of(ILLEGAL_CAST, this.getClass(), HitInversion.class);
    }

    public TerminateSCC asTerminateSCC() {
        throw TypeDBException.of(ILLEGAL_CAST, this.getClass(), TerminateSCC.class);
    }

    public enum MessageType {
        ANSWER,
        CONCLUSION,
        HIT_INVERSION,
        TERMINATE_SCC,
        DONE,
    }

    private Message(MessageType msgType, int index) {
        this.msgType = msgType;
        this.index = index;
    }

    public MessageType type() {
        return msgType;
    }

    public int index() {
        return index;
    }

    @Override
    public String toString() {
        return String.format("Msg:[%d: %s]", index, msgType.name());
    }

    public static class Answer extends Message {

        private final ConceptMap answer;

        public Answer(int index, ConceptMap answer) {
            super(MessageType.ANSWER, index);
            this.answer = answer;
        }

        public ConceptMap answer() {
            return answer;
        }

        @Override
        public Message.Answer asAnswer() {
            return this;
        }

        @Override
        public String toString() {
            return String.format("Msg:[%d: %s | %s]", index(), type().name(), answer);
        }
    }

    public static class Conclusion extends Message {

        private final Map<Identifier.Variable, Concept> conclusionAnswer;

        public Conclusion(int index, Map<Identifier.Variable, Concept> conclusionAnswer) {
            super(MessageType.CONCLUSION, index);
            this.conclusionAnswer = conclusionAnswer;
        }

        public Map<Identifier.Variable, Concept> conclusionAnswer() {
            return conclusionAnswer;
        }

        public Message.Conclusion asConclusion() {
            return this;
        }
    }

    public static class Done extends Message {

        public Done(int index) {
            super(MessageType.DONE, index);
        }
    }

    public static class HitInversion extends Message {
        public final int nodeId;
        public final boolean throughAllPaths;
         // As an optimisation, we pass index = -1 to avoid flooding when we know it's too early.
        public HitInversion(int nodeId, boolean throughAllPaths, int index) {
            super(MessageType.HIT_INVERSION, index);
            this.nodeId = nodeId;
            this.throughAllPaths = throughAllPaths;
        }


        public Message.HitInversion asHitInversion() {
            return this;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.nodeId, this.throughAllPaths);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            HitInversion other = (HitInversion) o;
            return super.equals(other) &&
                    nodeId == other.nodeId && throughAllPaths == other.throughAllPaths;
        }
    }

    public static class TerminateSCC extends Message {
        private final HitInversion expectedInversion;

        public TerminateSCC(HitInversion expectedInversion, int index) {
            super(MessageType.TERMINATE_SCC, index);
            this.expectedInversion = expectedInversion;
        }

        public TerminateSCC asTerminateSCC() {
            return this;
        }

        public HitInversion expectedInversion() {
            return expectedInversion;
        }
    }
}
