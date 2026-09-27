package ddsj.dds.condition;

import ddsj.dds.instance.InstanceState;
import ddsj.dds.instance.SampleState;
import ddsj.dds.instance.ViewState;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * ReadCondition with SQL-like query expression.
 * <p>
 * QueryCondition extends ReadCondition with a filter expression
 * that allows content-based filtering of samples.
 */
public final class QueryCondition extends ReadCondition {
    private final String queryExpression;
    private final AtomicReference<List<String>> queryParameters;

    /**
     * Creates a QueryCondition.
     *
     * @param dataReader the owning DataReader
     * @param sampleStates sample states to match
     * @param viewStates view states to match
     * @param instanceStates instance states to match
     * @param queryExpression the SQL-like filter expression
     * @param queryParameters the query parameters
     */
    public QueryCondition(Object dataReader,
                          Set<SampleState> sampleStates,
                          Set<ViewState> viewStates,
                          Set<InstanceState> instanceStates,
                          String queryExpression,
                          List<String> queryParameters) {
        super(dataReader, sampleStates, viewStates, instanceStates);
        this.queryExpression = Objects.requireNonNull(queryExpression, "queryExpression");
        this.queryParameters = new AtomicReference<>(
                queryParameters != null ? List.copyOf(queryParameters) : List.of());
    }

    /**
     * Returns the query expression.
     *
     * @return the expression
     */
    public String getQueryExpression() {
        return queryExpression;
    }

    /**
     * Returns the current query parameters.
     *
     * @return the parameters
     */
    public List<String> getQueryParameters() {
        return queryParameters.get();
    }

    /**
     * Sets new query parameters.
     *
     * @param params the new parameters
     */
    public void setQueryParameters(List<String> params) {
        this.queryParameters.set(params != null ? List.copyOf(params) : List.of());
    }
}
