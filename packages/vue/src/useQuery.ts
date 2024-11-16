import { ref, computed, watchEffect, type Ref, type ComputedRef } from 'vue';
import type {
  FetchResult,
  ClientQueryBuilder,
  CollectionNameFromModels,
  Models,
  SubscriptionOptions,
  TriplitClient,
  Unalias,
} from '@triplit/client';
import type { WorkerClient } from '@triplit/client/worker-client';

/**
 * A composable that subscribes to a query
 *
 * @param client - The client instance to query with
 * @param collectionName - The collection name to query
 * @param queryBuilder - A function that builds a query
 * @param options - Additional options for the subscription
 * @param options.localOnly - If true, the subscription will only use the local cache. Defaults to false.
 * @param options.onRemoteFulfilled - An optional callback that is called when the remote query has been fulfilled.
 * @returns An object containing the fetching state, the result of the query, any error that occurred, and a function to update the query
 */
export function useQuery<
  M extends Models,
  CollectionName extends CollectionNameFromModels<M>,
  QueryBuilder extends ClientQueryBuilder<M, CollectionName, any>,
  Query extends ReturnType<QueryBuilder['build']>
>(
  client: TriplitClient<M> | WorkerClient<M>,
  collectionName: CollectionName,
  queryBuilder: (q: QueryBuilder) => QueryBuilder,
  options?: Partial<SubscriptionOptions>
): {
  fetching: ComputedRef<boolean>;
  fetchingLocal: ComputedRef<boolean>;
  fetchingRemote: ComputedRef<boolean>;
  results: ComputedRef<Unalias<FetchResult<M, Query>> | undefined>;
  error: ComputedRef<unknown>;
  updateQuery: (newQueryBuilder: (q: QueryBuilder) => QueryBuilder) => void;
} {
  const query = queryBuilder(client.query(collectionName) as QueryBuilder);
  const results = ref<Unalias<FetchResult<M, Query>> | undefined>(
    undefined
  ) as Ref<Unalias<FetchResult<M, Query>> | undefined>;
  const isInitialFetch = ref(true);
  const fetchingLocal = ref(true);
  const fetchingRemote = ref(client.connectionStatus !== 'CLOSED');
  const fetching = computed(
    () => fetchingLocal.value || (isInitialFetch.value && fetchingRemote.value)
  );
  const error = ref<unknown>(undefined);
  let hasResponseFromServer = false;

  const builtQuery = ref(query.build()) as Ref<Query>;

  function updateQuery(newQueryBuilder: (q: QueryBuilder) => QueryBuilder) {
    const newQuery = newQueryBuilder(
      client.query(collectionName) as QueryBuilder
    );
    builtQuery.value = newQuery.build();
    results.value = undefined;
    fetchingLocal.value = true;
    hasResponseFromServer = false;
  }

  watchEffect(() => {
    client.isFirstTimeFetchingQuery(builtQuery.value).then((isFirstFetch) => {
      isInitialFetch.value = isFirstFetch;
    });
    const unsub = client.onConnectionStatusChange((status) => {
      if (status === 'CLOSING' || status === 'CLOSED') {
        fetchingRemote.value = false;
        return;
      }
      if (status === 'OPEN' && hasResponseFromServer === false) {
        fetchingRemote.value = true;
        return;
      }
    }, true);
    return () => {
      unsub();
    };
  });

  watchEffect(() => {
    const unsubscribe = client.subscribe(
      builtQuery.value,
      (newResults) => {
        fetchingLocal.value = false;
        error.value = undefined;
        results.value = newResults;
      },
      (err) => {
        fetchingLocal.value = false;
        error.value = err;
      },
      {
        ...(options ?? {}),
        onRemoteFulfilled: () => {
          hasResponseFromServer = true;
          fetchingRemote.value = false;
        },
      }
    );
    return () => {
      unsubscribe();
    };
  });

  return {
    fetching,
    fetchingLocal: computed(() => fetchingLocal.value),
    fetchingRemote: computed(() => fetchingRemote.value),
    results: computed(() => results.value),
    error: computed(() => error.value),
    updateQuery,
  };
}
