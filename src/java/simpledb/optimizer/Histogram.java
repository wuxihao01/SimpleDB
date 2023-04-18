package simpledb.optimizer;

import simpledb.execution.Predicate;

public interface Histogram<T> {

    void addValue(final T v);

    double estimateSelectivity(Predicate.Op op, T v);

    double avgSelectivity();

}
