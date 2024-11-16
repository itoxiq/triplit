import { useConnectionStatus } from './useConnectionStatus.js';
import { useQuery } from './useQuery.js';
import type {
  TriplitClient,
  Models,
  CollectionNameFromModels,
  ClientQuery,
  ClientQueryBuilder,
  SubscriptionOptions,
} from '@triplit/client';
import type { WorkerClient } from '@triplit/client/worker-client';

/**
 * A composable that provides access to Triplit client functionality
 *
 * @param client - The Triplit client instance to use
 * @returns An object containing wrapped versions of useQuery and useConnectionStatus composables
 */
export function useTriplit<
  M extends Models,
  CollectionName extends CollectionNameFromModels<M>,
  QueryBuilder extends ClientQueryBuilder<M, CollectionName, any>,
  Query extends ReturnType<QueryBuilder['build']>
>(client: TriplitClient<M> | WorkerClient<M>) {
  function useQueryWrapped(
    collectionName: CollectionName,
    queryBuilder: (q: QueryBuilder) => QueryBuilder,
    options?: Partial<SubscriptionOptions>
  ) {
    return useQuery<M, CollectionName, QueryBuilder, Query>(
      client,
      collectionName,
      queryBuilder,
      options
    );
  }

  function useConnectionStatusWrapped() {
    return useConnectionStatus(client);
  }

  return {
    useQuery: useQueryWrapped,
    useConnectionStatus: useConnectionStatusWrapped,
  };
}
