package com.vaticle.typedb.core.reasoner.v4.nodes;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.concept.answer.ConceptMap;
import com.vaticle.typedb.core.reasoner.v4.ActorNode;
import com.vaticle.typedb.core.reasoner.v4.Message;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class AnswerTable {

    private final List<Message> answers;
    private Set<ActorNode.Port> subscribers;
    private boolean complete;

    public AnswerTable() {
        this.answers = new ArrayList<>();
        this.subscribers = new HashSet<>();
        this.complete = false;
    }

    public int size() {
        return answers.size();
    }

    public Optional<Message> answerAt(int index) {
        assert index < answers.size() || (index == answers.size() && !complete);
        return index < answers.size() ? Optional.of(answers.get(index)) : Optional.empty();
    }

    public void registerSubscriber(ActorNode.Port subscriber, int index) {
        assert index == answers.size() && !complete;
        subscribers.add(subscriber);
    }

    public FunctionalIterator<ActorNode.Port> clearAndReturnSubscribers(int index) {
        assert index == answers.size() && !complete;
        Set<ActorNode.Port> subs = subscribers;
        subscribers = new HashSet<>();
        return Iterators.iterate(subs);
    }

    public Message recordAnswer(ConceptMap answer) {
        assert !complete;
        Message msg = Message.answer(answers.size(), answer);
        answers.add(msg);
        return msg;
    }

    public Message recordDone() {
        assert !complete;
        Message msg = Message.done(answers.size());
        answers.add(msg);
        this.complete = true;
        return msg;
    }
}