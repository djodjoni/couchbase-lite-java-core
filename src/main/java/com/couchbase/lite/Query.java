package com.couchbase.lite;

import com.couchbase.lite.internal.InterfaceAudience;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

/**
 * Represents a query of a CouchbaseLite 'view', or of a view-like resource like _all_documents.
 */
public class Query {

    /**
     * Determines whether or when the view index is updated. By default, the index will be updated
     * if necessary before the query runs -- this guarantees up-to-date results but can cause a delay.
     */
    public enum IndexUpdateMode {
        BEFORE,  // Always update index if needed before querying (default)
        NEVER,   // Don't update the index; results may be out of date
        AFTER    // Update index _after_ querying (results may still be out of date)
    }

    /**
     * Changes the behavior of a query created by queryAllDocuments.
     */
    public enum AllDocsMode {
        ALL_DOCS,          // (the default), the query simply returns all non-deleted documents.
        INCLUDE_DELETED,   // in this mode it also returns deleted documents.
        SHOW_CONFLICTS,    // the .conflictingRevisions property of each row will return the conflicting revisions, if any, of that document.
        ONLY_CONFLICTS     // _only_ documents in conflict will be returned. (This mode is especially useful for use with a CBLLiveQuery, so you can be notified of conflicts as they happen, i.e. when they're pulled in by a replication.)
    }

    private Database database;

    private View view;  // null for _all_docs query

    /**
     * Is this query based on a temporary view?
     */
    private boolean temporaryView;

    private int skip;
    private int limit = Integer.MAX_VALUE;
    private Object startKey;
    private Object endKey;
    private String startKeyDocId;
    private String endKeyDocId;
    private IndexUpdateMode indexUpdateMode;
    private AllDocsMode allDocsMode;
    private boolean descending;
    private boolean prefetch;
    private boolean mapOnly;
    private boolean includeDeleted;
    private List<Object> keys;
    private int groupLevel;
    private long lastSequence;

    /**
     * If a query is running and the user calls stop() on this query, the future
     * will be used in order to cancel the query in progress.
     */
    protected Future updateQueryFuture;

    /**
     * Constructor
     */
    @InterfaceAudience.Private
    /* package */ Query(Database database, View view) {
        this.database = database;
        this.view = view;
        limit = Integer.MAX_VALUE;
        mapOnly = (view != null && view.getReduce() == null);
        indexUpdateMode = IndexUpdateMode.BEFORE;
        allDocsMode = AllDocsMode.ALL_DOCS;
    }

    /**
     * Constructor
     */
    @InterfaceAudience.Private
    /* package */ Query(Database database, Mapper mapFunction) {
        this(database, database.makeAnonymousView());
        temporaryView = true;
        view.setMap(mapFunction, "");
    }

    /**
     * Constructor
     */
    @InterfaceAudience.Private
    /* package */ Query(Database database, Query query) {
        this(database, query.getView());
        limit = query.limit;
        skip = query.skip;
        startKey = query.startKey;
        endKey = query.endKey;
        descending = query.descending;
        prefetch = query.prefetch;
        keys = query.keys;
        groupLevel = query.groupLevel;
        mapOnly = query.mapOnly;
        startKeyDocId = query.startKeyDocId;
        endKeyDocId = query.endKeyDocId;
        indexUpdateMode = query.indexUpdateMode;
        allDocsMode = query.allDocsMode;
    }

    /**
     * The database this query is associated with
     */
    @InterfaceAudience.Public
    public Database getDatabase() {
        return database;
    }

    /**
     * Gets the maximum number of rows to return. The default value is 0, meaning 'unlimited'.
     */
    @InterfaceAudience.Public
    public int getLimit() {
        return limit;
    }

    /**
     * Sets the maximum number of rows to return. The default value is 0, meaning 'unlimited'.
     */
    @InterfaceAudience.Public
    public void setLimit(int limit) {
        this.limit = limit;
    }

    /**
     * Gets the number of initial rows to skip. Default value is 0.
     */
    @InterfaceAudience.Public
    public int getSkip() {
        return skip;
    }

    /**
     * Sets the number of initial rows to skip. Default value is 0.
     */
    @InterfaceAudience.Public
    public void setSkip(int skip) {
        this.skip = skip;
    }

    /**
     * Gets whether the rows be returned in descending key order. Default value is false.
     */
    @InterfaceAudience.Public
    public boolean isDescending() {
        return descending;
    }

    /**
     * Sets whether the rows be returned in descending key order. Default value is false.
     */
    @InterfaceAudience.Public
    public void setDescending(boolean descending) {
        this.descending = descending;
    }

    /**
     * Gets the key of the first value to return. A null value has no effect.
     */
    @InterfaceAudience.Public
    public Object getStartKey() {
        return startKey;
    }

    /**
     * Sets the key of the first value to return. A null value has no effect.
     */
    @InterfaceAudience.Public
    public void setStartKey(Object startKey) {
        this.startKey = startKey;
    }

    /**
     * Gets the key of the last value to return. A null value has no effect.
     */
    @InterfaceAudience.Public
    public Object getEndKey() {
        return endKey;
    }

    /**
     * Sets the key of the last value to return. A null value has no effect.
     */
    @InterfaceAudience.Public
    public void setEndKey(Object endKey) {
        this.endKey = endKey;
    }

    /**
     * Gets the Document id of the first value to return.
     * A null value has no effect. This is useful if the view contains multiple
     * identical keys, making startKey ambiguous.
     */
    @InterfaceAudience.Public
    public String getStartKeyDocId() {
        return startKeyDocId;
    }

    /**
     * Sets the Document id of the first value to return.
     * A null value has no effect. This is useful if the view contains multiple
     * identical keys, making startKey ambiguous.
     */
    @InterfaceAudience.Public
    public void setStartKeyDocId(String startKeyDocId) {
        this.startKeyDocId = startKeyDocId;
    }


    /**
     * Gets the Document id of the last value to return. A null value has no effect.
     * This is useful if the view contains multiple identical keys, making endKey ambiguous.
     */
    @InterfaceAudience.Public
    public String getEndKeyDocId() {
        return endKeyDocId;
    }

    /**
     * Sets the Document id of the last value to return. A null value has no effect.
     * This is useful if the view contains multiple identical keys, making endKey ambiguous.
     */
    @InterfaceAudience.Public
    public void setEndKeyDocId(String endKeyDocId) {
        this.endKeyDocId = endKeyDocId;
    }

    /**
     * Gets when a View index is updated when running a Query.
     */
    @InterfaceAudience.Public
    public IndexUpdateMode getIndexUpdateMode() {
        return indexUpdateMode;
    }

    /**
     * Sets when a View index is updated when running a Query.
     */
    @InterfaceAudience.Public
    public void setIndexUpdateMode(IndexUpdateMode indexUpdateMode) {
        this.indexUpdateMode = indexUpdateMode;
    }

    /**
     * Changes the behavior of a query created by -queryAllDocuments.
     *
     * - In mode kCBLAllDocs (the default), the query simply returns all non-deleted documents.
     * - In mode kCBLIncludeDeleted, it also returns deleted documents.
     * - In mode kCBLShowConflicts, the .conflictingRevisions property of each row will return the
     *   conflicting revisions, if any, of that document.
     * - In mode kCBLOnlyConflicts, _only_ documents in conflict will be returned.
     *   (This mode is especially useful for use with a CBLLiveQuery, so you can be notified of
     *   conflicts as they happen, i.e. when they're pulled in by a replication.)
     */
    @InterfaceAudience.Public
    public AllDocsMode getAllDocsMode() {
        return allDocsMode;
    }

    /**
     * See getAllDocsMode()
     */
    @InterfaceAudience.Public
    public void setAllDocsMode(AllDocsMode allDocsMode) {
        this.allDocsMode = allDocsMode;
    }


    /**
     * Gets the keys of the values to return. A null value has no effect.
     */
    @InterfaceAudience.Public
    public List<Object> getKeys() {
        return keys;
    }

    /**
     * Sets the keys of the values to return. A null value has no effect.
     */
    @InterfaceAudience.Public
    public void setKeys(List<Object> keys) {
        this.keys = keys;
    }

    /**
     * Gets whether to only use the map function without using the reduce function.
     */
    @InterfaceAudience.Public
    public boolean isMapOnly() {
        return mapOnly;
    }

    /**
     * Sets whether to only use the map function without using the reduce function.
     */
    @InterfaceAudience.Public
    public void setMapOnly(boolean mapOnly) {
        this.mapOnly = mapOnly;
    }

    /**
     * Gets whether results will be grouped in Views that have reduce functions.
     */
    @InterfaceAudience.Public
    public int getGroupLevel() {
        return groupLevel;
    }

    /**
     * Sets whether results will be grouped in Views that have reduce functions.
     */
    @InterfaceAudience.Public
    public void setGroupLevel(int groupLevel) {
        this.groupLevel = groupLevel;
    }

    /**
     * Gets whether to include the entire Document content with the results.
     * The Documents can be accessed via the QueryRow's documentProperties property.
     */
    @InterfaceAudience.Public
    public boolean shouldPrefetch() {
        return prefetch;
    }

    /**
     * Sets whether to include the entire Document content with the results.
     * The Documents can be accessed via the QueryRow's documentProperties property.
     */
    @InterfaceAudience.Public
    public void setPrefetch(boolean prefetch) {
        this.prefetch = prefetch;
    }

    /**
     * Gets whether Queries created via the Database createAllDocumentsQuery
     * method will include deleted Documents. This property has no effect in other types of Queries.
     */
    @InterfaceAudience.Public
    public boolean shouldIncludeDeleted() {
        return allDocsMode == AllDocsMode.INCLUDE_DELETED;
    }

    /**
     * Sets whether Queries created via the Database createAllDocumentsQuery
     * method will include deleted Documents. This property has no effect in other types of Queries.
     */
    @InterfaceAudience.Public
    public void setIncludeDeleted(boolean includeDeletedParam) {
        allDocsMode = (includeDeletedParam == true) ? AllDocsMode.INCLUDE_DELETED : AllDocsMode.ALL_DOCS;
    }

    /**
     * Sends the query to the server and returns an enumerator over the result rows (Synchronous).
     * If the query fails, this method returns nil and sets the query's .error property.
     */
    @InterfaceAudience.Public
    public QueryEnumerator run() throws CouchbaseLiteException {
        List<Long> outSequence = new ArrayList<Long>();
        String viewName = (view != null) ? view.getName() : null;
        List<QueryRow> rows = database.queryViewNamed(viewName, getQueryOptions(), outSequence);
        lastSequence = outSequence.get(0);
        return new QueryEnumerator(database, rows, lastSequence);
    }

    /**
     * Returns a live query with the same parameters.
     */
    @InterfaceAudience.Public
    public LiveQuery toLiveQuery() {
        if (view == null) {
            throw new IllegalStateException("Cannot convert a Query to LiveQuery if the view is null");
        }
        return new LiveQuery(this);
    }

    /**
     *  Starts an asynchronous query. Returns immediately, then calls the onLiveQueryChanged block when the
     *  query completes, passing it the row enumerator. If the query fails, the block will receive
     *  a non-nil enumerator but its .error property will be set to a value reflecting the error.
     *  The originating Query's .error property will NOT change.
     */
    @InterfaceAudience.Public
    public Future runAsync(final QueryCompleteListener onComplete) {
        return runAsyncInternal(onComplete);
    }

    /**
     * A delegate that can be called to signal the completion of a Query.
     */
    @InterfaceAudience.Public
    public static interface QueryCompleteListener {
        public void completed(QueryEnumerator rows, Throwable error);
    }

    /**
     * @exclude
     */
    @InterfaceAudience.Private
    Future runAsyncInternal(final QueryCompleteListener onComplete) {

        return database.getManager().runAsync(new Runnable() {
            @Override
            public void run() {
                try {
                    String viewName = view.getName();
                    QueryOptions options = getQueryOptions();
                    List<Long> outSequence = new ArrayList<Long>();
                    List<QueryRow> rows = database.queryViewNamed(viewName, options, outSequence);
                    long sequenceNumber = outSequence.get(0);
                    QueryEnumerator enumerator = new QueryEnumerator(database, rows, sequenceNumber);
                    onComplete.completed(enumerator, null);

                } catch (Throwable t) {
                    onComplete.completed(null, t);
                }
            }
        });

    }

    /**
     * The view object associated with this query
     * @exclude
     */
    @InterfaceAudience.Private
    public View getView() {
        return view;
    }

    @InterfaceAudience.Private
    private QueryOptions getQueryOptions() {
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.setStartKey(getStartKey());
        queryOptions.setEndKey(getEndKey());
        queryOptions.setStartKey(getStartKey());
        queryOptions.setKeys(getKeys());
        queryOptions.setSkip(getSkip());
        queryOptions.setLimit(getLimit());
        queryOptions.setReduce(!isMapOnly());
        queryOptions.setReduceSpecified(true);
        queryOptions.setGroupLevel(getGroupLevel());
        queryOptions.setDescending(isDescending());
        queryOptions.setIncludeDocs(shouldPrefetch());
        queryOptions.setUpdateSeq(true);
        queryOptions.setInclusiveEnd(true);
        queryOptions.setStale(getIndexUpdateMode());
        queryOptions.setAllDocsMode(getAllDocsMode());
        return queryOptions;
    }

    @Override
    @InterfaceAudience.Private
    protected void finalize() throws Throwable {
        super.finalize();
        if (temporaryView) {
            view.delete();
        }
    }




}
