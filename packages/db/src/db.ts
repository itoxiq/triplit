import {
  AttributeItem,
  EAV,
  StoreSchema,
  TripleRow,
  TripleStore,
  TripleStoreTransaction,
} from './triple-store';
import {
  getSchemaFromPath,
  JSONTypeFromModel,
  Model,
  Models,
  timestampedObjectToPlainObject,
  objectToTimestampedObject,
  TypeFromModel,
} from './schema';
import * as Document from './document';
import { nanoid } from 'nanoid';
import { AsyncTupleStorageApi } from 'tuple-database';
import CollectionQueryBuilder, {
  CollectionQuery,
  doesEntityObjMatchWhere,
  fetch,
  FetchResult,
  subscribe,
  subscribeTriples,
} from './collection-query';
import { FilterStatement, Query, QueryWhere } from './query';
import MemoryStorage from './storage/memory-btree';
import {
  EntityNotFoundError,
  InvalidEntityIdError,
  InvalidInternalEntityIdError,
  InvalidMigrationOperationError,
  SessionVariableNotFoundError,
  WriteRuleError,
} from './errors';
import { Clock } from './clocks/clock';

type Reference = `ref:${string}`;
import { ValuePointer } from '@sinclair/typebox/value';

type AttributeType =
  | 'string'
  | 'number'
  | 'boolean'
  | 'set_string'
  | 'set_number'
  | 'record'
  | Reference;

type CollectionAttribute = {
  type: AttributeType;
};

export interface Rule<M extends Model<any>> {
  filter: QueryWhere<M>;
  description?: string;
}

export interface CollectionRules {
  read?: Rule<any>[];
  write?: Rule<any>[];
  // insert?: Rule<any>[];
  // update?: Rule<any>[];
}

type CreateCollectionOperation = [
  'create_collection',
  {
    name: string;
    attributes: { [path: string]: CollectionAttribute };
    rules: CollectionRules;
  }
];
type DropCollectionOperation = ['drop_collection', { name: string }];
type AddAttributeOperation = [
  'add_attribute',
  { collection: string; path: string; attribute: CollectionAttribute }
];
type DropAttributeOperation = [
  'drop_attribute',
  { collection: string; path: string }
];
// TODO: rename path should be string[] not string
type RenameAttributeOperation = [
  'rename_attribute',
  { collection: string; path: string; newPath: string }
];
type DBOperation =
  | CreateCollectionOperation
  | DropCollectionOperation
  | AddAttributeOperation
  | DropAttributeOperation
  | RenameAttributeOperation;

export type Migration = {
  up: DBOperation[];
  down: DBOperation[];
  version: number;
  parent: number;
};

type StorageSource = AsyncTupleStorageApi;

interface DBConfig<M extends Models<any, any> | undefined> {
  schema?: { collections: M; version?: number };
  migrations?: Migration[];
  source?: StorageSource;
  sources?: Record<string, StorageSource>;
  tenantId?: string;
  clock?: Clock;
  variables?: Record<string, any>;
}

const DEFAULT_STORE_KEY = 'default';

export type ModelFromModels<
  M extends Models<any, any> | undefined,
  CN extends CollectionNameFromModels<M> = any
> = M extends Models<any, any>
  ? M[CN]
  : M extends undefined
  ? undefined
  : never;

export type CollectionNameFromModels<M extends Models<any, any> | undefined> =
  M extends Models<any, any> ? keyof M : M extends undefined ? string : never;

function ruleToTuple(
  collectionName: string,
  ruleType: keyof CollectionRules,
  index: number,
  rule: Rule<any>
) {
  return Object.entries(rule).map<EAV>(([key, value]) => [
    '_schema',
    ['collections', collectionName, 'rules', ruleType, index, key],
    value,
  ]);
}

export class DBTransaction<M extends Models<any, any> | undefined> {
  constructor(
    readonly storeTx: TripleStoreTransaction,
    readonly variables?: Record<string, any>
  ) {}

  // get schema() {
  //   return this.storeTx.schema?.collections;
  // }

  async getCollectionSchema<CN extends CollectionNameFromModels<M>>(
    collectionName: CN
  ) {
    const { collections } = (await this.getSchema()) ?? {};
    if (!collections) return undefined;
    // TODO: i think we need some stuff in the triple store...
    const collectionSchema = collections[collectionName] as ModelFromModels<
      M,
      CN
    >;
    return {
      ...collectionSchema,
    };
  }

  private addReadRulesToQuery(
    query: CollectionQuery<ModelFromModels<M>>,
    schema: M
  ): CollectionQuery<ModelFromModels<M>> {
    if (schema?.rules?.read) {
      const updatedWhere = [
        ...query.where,
        ...schema.rules.read.flatMap((rule) => rule.filter),
      ];
      return { ...query, where: updatedWhere };
    }
    return query;
  }

  async getSchema() {
    return this.storeTx.readSchema();
  }

  async commit() {
    await this.storeTx.commit();
  }

  async cancel() {
    await this.storeTx.cancel();
  }

  async insert(
    collectionName: CollectionNameFromModels<M>,
    doc: any,
    id?: string
  ) {
    if (id) {
      const validationError = validateExternalId(id);
      if (validationError) throw validationError;
    }
    const schema = await this.getCollectionSchema(collectionName);

    if (schema?.rules?.write?.length) {
      const filters = schema.rules.write.flatMap((r) => r.filter);
      let query = { where: filters } as CollectionQuery<ModelFromModels<M>>;
      query = this.replaceVariablesInQuery(query);
      // TODO there is probably a better way to to this
      // rather than converting to timestamped object check to
      // validate the where filter
      const timestampDoc = objectToTimestampedObject(doc);
      const satisfiedRule = doesEntityObjMatchWhere(
        timestampDoc,
        query.where,
        schema
      );
      if (!satisfiedRule) {
        // TODO add better error that uses rule description
        throw new WriteRuleError(`Insert does not match write rules`);
      }
    }
    await Document.insert(
      this.storeTx,
      appendCollectionToId(collectionName, id ?? nanoid()),
      doc,
      collectionName
    );
  }

  async update<CN extends CollectionNameFromModels<M>>(
    collectionName: CN,
    entityId: string,
    updater: (
      entity: JSONTypeFromModel<ModelFromModels<M, CN>>
    ) => Promise<void>
  ) {
    const schema = (await this.getSchema())?.collections[collectionName] as
      | ModelFromModels<M, CN>
      | undefined;

    const entity = await this.fetchById(collectionName, entityId);

    if (!entity) {
      throw new EntityNotFoundError(
        entityId,
        collectionName,
        "Cannot perform an update on an entity that doesn't exist"
      );
    }
    const changes = new Map<string, any>();
    const updateProxy = this.createUpdateProxy<typeof schema>(
      changes,
      entity,
      schema
    );
    await updater(updateProxy);
    const fullEntityId = appendCollectionToId(collectionName, entityId);
    for (let [path, value] of changes) {
      await this.storeTx.setValue(
        fullEntityId,
        [collectionName, ...path.slice(1).split('/')],
        value
      );
    }
    if (schema?.rules?.write?.length) {
      const updatedEntity = await this.fetchById(collectionName, entityId);
      const filters = schema.rules.write.flatMap((r) => r.filter);
      let query = { where: filters } as CollectionQuery<ModelFromModels<M>>;
      query = this.replaceVariablesInQuery(query);
      const satisfiedRule = doesEntityObjMatchWhere(
        objectToTimestampedObject(updatedEntity),
        query.where,
        schema
      );
      if (!satisfiedRule) {
        // TODO add better error that uses rule description
        throw new WriteRuleError(`Update does not match write rules`);
      }
    }
  }

  private createUpdateProxy<M extends Model<any> | undefined>(
    changeTracker: Map<string, any>,
    entityObj: JSONTypeFromModel<M>,
    schema?: M,
    prefix: string = ''
  ): JSONTypeFromModel<M> {
    return new Proxy(entityObj, {
      set: (_target, prop, value) => {
        const propPointer = [prefix, prop].join('/');
        if (!schema) {
          changeTracker.set(propPointer, value);
          return true;
        }
        const propSchema = getSchemaFromPath(
          schema,
          propPointer.slice(1).split('/')
        );
        if (!propSchema) {
          // TODO use correct Triplit Error
          throw new Error(
            `Cannot set unrecognized property ${propPointer} to ${value}`
          );
        }
        changeTracker.set(propPointer, value);
        return true;
      },
      get: (_target, prop) => {
        const propPointer = [prefix, prop].join('/');
        const propValue = ValuePointer.Get(entityObj, propPointer);
        if (propValue === undefined) return changeTracker.get(propPointer);
        const propSchema =
          schema && getSchemaFromPath(schema, propPointer.slice(1).split('/'));
        if (
          typeof propValue === 'object' &&
          (!propSchema || propSchema['x-crdt-type'] !== 'Set') &&
          propValue !== null
        ) {
          return this.createUpdateProxy(
            changeTracker,
            propValue,
            schema,
            propPointer
          );
        }
        if (propSchema) {
          if (propSchema['x-crdt-type'] === 'Set') {
            return {
              add: (value: any) => {
                changeTracker.set([propPointer, value].join('/'), true);
              },
              remove: (value: any) => {
                changeTracker.set([propPointer, value].join('/'), false);
              },
              has: (value: any) => {
                const valuePointer = [propPointer, value].join('/');
                return changeTracker.has(valuePointer)
                  ? changeTracker.get(valuePointer)
                  : propValue[value];
              },
            };
          }
        }
        return changeTracker.has(propPointer)
          ? changeTracker.get(propPointer)
          : propValue;
      },
    });
  }

  private replaceVariablesInQuery(
    query: CollectionQuery<ModelFromModels<M>>
  ): CollectionQuery<ModelFromModels<M>> {
    const variables = { ...(this.variables ?? {}), ...(query.vars ?? {}) };
    const where = replaceVariablesInFilterStatements(query.where, variables);
    return { ...query, where };
  }

  async fetch(query: CollectionQuery<ModelFromModels<M>>) {
    let fetchQuery = query;
    const schema = await this.getCollectionSchema(
      fetchQuery.collectionName as CollectionNameFromModels<M>
    );
    if (schema) {
      fetchQuery = this.addReadRulesToQuery(fetchQuery, schema);
    }
    fetchQuery = this.replaceVariablesInQuery(fetchQuery);
    return fetch(this.storeTx, fetchQuery, { schema, includeTriples: false });
  }

  async fetchById(collectionName: CollectionNameFromModels<M>, id: string) {
    const schema = await this.getCollectionSchema(collectionName);
    const readRules = schema?.rules?.read;
    const entity = await this.storeTx.getEntity(
      appendCollectionToId(collectionName, id)
    );
    if (!entity) return null;
    if (entity && readRules) {
      const whereFilter = readRules.flatMap((rule) => rule.filter);
      let query = { where: whereFilter };
      /**
       * TODO we should just make this operate directly on where filters
       * e.g.
       * query.where = this.replaceVariablesInWhere(query.where)
       */
      // @ts-ignore
      query = this.replaceVariablesInQuery(query);
      if (doesEntityObjMatchWhere(entity, query.where, schema)) {
        return entity;
      }
      return null;
    }
    return timestampedObjectToPlainObject(entity) as TypeFromModel<
      M[typeof collectionName]
    >;
  }

  async createCollection(params: CreateCollectionOperation[1]) {
    const { name: collectionName, attributes, rules } = params;
    const attributeTuples = Object.entries(attributes).map<EAV>(
      ([path, attribute]) => [
        '_schema',
        ['collections', collectionName, 'attributes', path, 'type'],
        attribute.type,
      ]
    );
    const ruleTuples = !rules
      ? []
      : (['read', 'write', 'update'] as (keyof CollectionRules)[]).flatMap(
          (ruleType) =>
            rules[ruleType] != undefined
              ? rules[ruleType]!.flatMap((rule, i) =>
                  ruleToTuple(collectionName, ruleType, i, rule)
                )
              : []
        );
    await this.storeTx.updateMetadataTuples([
      ...attributeTuples,
      ...ruleTuples,
    ]);
  }

  async dropCollection(params: DropCollectionOperation[1]) {
    const { name: collectionName } = params;
    // DELETE SCHEMA INFO
    const existingAttributeInfo = await this.storeTx.readMetadataTuples(
      '_schema',
      ['collections', collectionName]
    );
    const deletes = existingAttributeInfo.map<[string, AttributeItem[]]>(
      (eav) => [eav[0], eav[1]]
    );
    await this.storeTx.deleteMetadataTuples(deletes);

    // DELETE DATA
    // TODO: check _collection marker too?
    // const attribute = [collectionName];
    // const currentTriples = this.storeTx.findByAttribute(attribute);
    // this.storeTx.deleteTriples(currentTriples);
  }

  async renameAttribute(params: RenameAttributeOperation[1]) {
    const { collection: collectionName, path, newPath } = params;
    // Update schema if there is schema
    if (await this.getSchema()) {
      const existingAttributeInfo = await this.storeTx.readMetadataTuples(
        '_schema',
        ['collections', collectionName, 'attributes', path]
      );
      // Delete old attribute tuples
      const deletes = existingAttributeInfo.map<[string, AttributeItem[]]>(
        (eav) => [eav[0], eav[1]]
      );
      // Upsert new attribute tuples
      const updates = existingAttributeInfo.map<EAV>((eav) => {
        const attr = [...eav[1]];
        // ['collections', collectionName, 'attributes'] is prefix
        attr.splice(3, 1, newPath); // Logic may change if path and new path arent strings
        return [eav[0], attr, eav[2]];
      });
      await this.storeTx.deleteMetadataTuples(deletes);
      await this.storeTx.updateMetadataTuples(updates);
    }
    // Update data in place
    // For each storage scope, find all triples with the attribute and update them
    for (const storageKey of Object.keys(this.storeTx.tupleTx.store.storage)) {
      const attribute = [collectionName, path];
      const newAttribute = [collectionName, newPath];
      const scopedTx = this.storeTx.withScope({
        read: [storageKey],
        write: [storageKey],
      });
      const currentTriples = await scopedTx.findByAttribute(attribute);
      const newTriples = transformTripleAttribute(
        currentTriples,
        attribute,
        newAttribute
      );
      await scopedTx.deleteTriples(currentTriples);
      await scopedTx.insertTriples(newTriples);
    }
  }

  async addAttribute(params: AddAttributeOperation[1]) {
    const { collection: collectionName, path, attribute } = params;
    // Update schema if there is schema
    if (await this.getSchema()) {
      const updates: EAV[] = Object.entries(attribute).map(([key, value]) => {
        return [
          '_schema',
          ['collections', collectionName, 'attributes', path, key],
          value,
        ];
      });
      await this.storeTx.updateMetadataTuples(updates);
    }
  }

  async dropAttribute(params: DropAttributeOperation[1]) {
    const { collection: collectionName, path } = params;
    // Update schema if there is schema
    if (await this.getSchema()) {
      const existingAttributeInfo = await this.storeTx.readMetadataTuples(
        '_schema',
        ['collections', collectionName, 'attributes', path]
      );
      // Delete old attribute tuples
      const deletes = existingAttributeInfo.map<[string, AttributeItem[]]>(
        (eav) => [eav[0], eav[1]]
      );
      await this.storeTx.deleteMetadataTuples(deletes);
    }

    // TODO: check _collection marker too?
    // const attribute = [collectionName, path];
    // const currentTriples = this.storeTx.findByAttribute(attribute);
    // this.storeTx.deleteTriples(currentTriples);
  }
}

export default class DB<M extends Models<any, any> | undefined> {
  tripleStore: TripleStore;
  ensureMigrated: Promise<void>;
  variables: Record<string, any>;

  constructor({
    schema,
    source,
    sources,
    tenantId,
    migrations,
    clock,
    variables,
  }: DBConfig<M> = {}) {
    this.variables = variables ?? {};
    // If only one source is provided, use the default key
    const sourcesMap = sources ?? {
      [DEFAULT_STORE_KEY]: source ?? new MemoryStorage(),
    };
    if (Object.keys(sourcesMap).length === 0)
      throw new Error('No triple stores provided.');

    if (schema && migrations)
      throw new Error('Cannot provide both schema and migrations');

    // If a schema is provided, assume using schema but no migrations (keep at version 0)

    const tripleStoreSchema = schema
      ? { version: schema.version ?? 0, collections: schema.collections }
      : undefined;

    this.tripleStore = new TripleStore({
      storage: sourcesMap,
      tenantId,
      schema: tripleStoreSchema,
      clock,
    });

    this.ensureMigrated = migrations
      ? this.migrate(migrations, 'up').catch(() => {})
      : Promise.resolve();
  }

  async getClientId() {
    const ts = await this.tripleStore.clock.getCurrentTimestamp();
    return ts[1];
  }

  async getSchema(full: true): Promise<StoreSchema<M>>;
  async getSchema(
    full?: false
  ): Promise<
    M extends Models<any, any>
      ? StoreSchema<M>['collections']
      : M extends undefined
      ? undefined
      : never
  >;
  async getSchema(full: boolean = false) {
    await this.ensureMigrated;
    const tripleStoreSchema = await this.tripleStore.readSchema();
    if (full) return tripleStoreSchema;
    return tripleStoreSchema?.collections;
  }

  async getCollectionSchema<CN extends CollectionNameFromModels<M>>(
    collectionName: CN
  ) {
    const collections = await this.getSchema();
    if (!collections) return undefined;
    // TODO: i think we need some stuff in the triple store...
    const collectionSchema = collections[collectionName] as ModelFromModels<
      M,
      CN
    >;
    return {
      ...collectionSchema,
    };
  }

  static ABORT_TRANSACTION = Symbol('abort transaction');

  private addReadRulesToQuery(
    query: CollectionQuery<ModelFromModels<M>>,
    schema: M
  ): CollectionQuery<ModelFromModels<M>> {
    if (schema?.rules?.read) {
      const updatedWhere = [
        ...query.where,
        ...schema.rules.read.flatMap((rule) => rule.filter),
      ];
      return { ...query, where: updatedWhere };
    }
    return query;
  }

  async transact(
    callback: (tx: DBTransaction<M>) => Promise<void>,
    storeScope?: { read: string[]; write: string[] }
  ) {
    await this.ensureMigrated;
    return await this.tripleStore.transact(async (tripTx) => {
      const tx = new DBTransaction<M>(tripTx, this.variables);
      try {
        await callback(tx);
      } catch (e) {
        console.error(e);
        await tx.cancel();
        throw e;
      }
    }, storeScope);
  }

  updateVariables(variables: Record<string, any>) {
    this.variables = { ...this.variables, ...variables };
  }

  private replaceVariablesInQuery(
    query: CollectionQuery<ModelFromModels<M>>
  ): CollectionQuery<ModelFromModels<M>> {
    const variables = { ...(this.variables ?? {}), ...(query.vars ?? {}) };
    const where = replaceVariablesInFilterStatements(query.where, variables);
    return { ...query, where };
  }

  async fetch(query: CollectionQuery<ModelFromModels<M>>, scope?: string[]) {
    await this.ensureMigrated;
    // TODO: need to fix collectionquery typing
    let fetchQuery = query;
    const schema = await this.getCollectionSchema(
      fetchQuery.collectionName as CollectionNameFromModels<M>
    );
    if (schema) {
      fetchQuery = this.addReadRulesToQuery(fetchQuery, schema);
    }
    fetchQuery = this.replaceVariablesInQuery(fetchQuery);
    return await fetch(
      scope ? this.tripleStore.setStorageScope(scope) : this.tripleStore,
      fetchQuery,
      { schema, includeTriples: false }
    );
  }

  // TODO: we could probably infer a type here
  async fetchById<Schema extends Model<any>>(
    collectionName: CollectionNameFromModels<M>,
    id: string
  ) {
    const schema = await this.getCollectionSchema(collectionName);
    const readRules = schema?.rules?.read;
    const entity = await this.tripleStore.getEntity(
      appendCollectionToId(collectionName, id)
    );
    if (!entity) return null;
    if (entity && readRules) {
      const whereFilter = readRules.flatMap((rule) => rule.filter);
      let query = { where: whereFilter };
      // TODO see other comment about replaceVariablesInQuery on how to improve
      // @ts-ignore
      query = this.replaceVariablesInQuery(query);
      if (doesEntityObjMatchWhere(entity, query.where, schema)) {
        return entity;
      }
      return null;
    }
    return timestampedObjectToPlainObject(
      entity
    ) as TypeFromModel<Schema> | null;
  }

  async insert(
    collectionName: CollectionNameFromModels<M>,
    doc: any,
    id?: string,
    storeScope?: { read: string[]; write: string[] }
  ) {
    if (id) {
      const validationError = validateExternalId(id);
      if (validationError) throw validationError;
    }
    await this.ensureMigrated;
    const schema = await this.getCollectionSchema(collectionName);

    if (schema?.rules?.write?.length) {
      const filters = schema.rules.write.flatMap((r) => r.filter);
      let query = { where: filters } as CollectionQuery<ModelFromModels<M>>;
      query = this.replaceVariablesInQuery(query);
      // TODO there is probably a better way to to this
      // rather than converting to timestamped object check to
      // validate the where filter
      const timestampDoc = objectToTimestampedObject(doc);
      const satisfiedRule = doesEntityObjMatchWhere(
        timestampDoc,
        query.where,
        schema
      );
      if (!satisfiedRule) {
        // TODO add better error that uses rule description
        throw new WriteRuleError(`Insert does not match write rules`);
      }
    }

    const timestamp = await this.tripleStore.transact(async (tx) => {
      await Document.insert(
        tx,
        appendCollectionToId(collectionName, id ?? nanoid()),
        doc,
        collectionName
      );
    }, storeScope);
    return timestamp;
  }

  subscribe<Q extends CollectionQuery<ModelFromModels<M>>>(
    query: Q,
    callback: (results: FetchResult<Q>) => void,
    scope?: string[]
  ) {
    const startSubscription = async () => {
      let subscriptionQuery = query;
      // TODO: get rid of this "as" here
      const schema = await this.getCollectionSchema(
        subscriptionQuery.collectionName as CollectionNameFromModels<M>
      );
      if (schema) {
        // TODO see other comment about replaceVariablesInQuery on how to improve
        // @ts-ignore
        subscriptionQuery = this.addReadRulesToQuery(subscriptionQuery, schema);
      }
      subscriptionQuery = this.replaceVariablesInQuery(subscriptionQuery);

      const unsub = subscribe(
        scope ? this.tripleStore.setStorageScope(scope) : this.tripleStore,
        subscriptionQuery,
        callback,
        schema
      );
      return unsub;
    };

    const unsubPromise = startSubscription();

    return async () => {
      const unsub = await unsubPromise;
      return unsub();
    };
  }

  subscribeTriples<Q extends CollectionQuery<ModelFromModels<M>>>(
    query: Q,
    callback: (results: TripleRow[]) => void,
    scope?: string[]
  ) {
    const startSubscription = async () => {
      let subscriptionQuery = query;
      const schema = await this.getCollectionSchema(
        subscriptionQuery.collectionName as CollectionNameFromModels<M>
      );
      if (schema) {
        subscriptionQuery = this.addReadRulesToQuery(subscriptionQuery, schema);
      }
      subscriptionQuery = this.replaceVariablesInQuery(subscriptionQuery);

      const unsub = subscribeTriples(
        scope ? this.tripleStore.setStorageScope(scope) : this.tripleStore,
        subscriptionQuery,
        callback,
        schema
      );
      return unsub;
    };

    const unsubPromise = startSubscription();

    return async () => {
      const unsub = await unsubPromise;
      return unsub();
    };
  }

  async update<CN extends CollectionNameFromModels<M>>(
    collectionName: CN,
    entityId: string,
    updater: (
      entity: JSONTypeFromModel<ModelFromModels<M, CN>>
    ) => Promise<void>,
    storeScope?: { read: string[]; write: string[] }
  ) {
    await this.ensureMigrated;
    return await this.transact(async (tx) => {
      await tx.update(collectionName, entityId, updater);
    }, storeScope);
  }

  query<CN extends CollectionNameFromModels<M>>(
    collectionName: CN,
    params?: Query<ModelFromModels<M, CN>>
  ) {
    return CollectionQueryBuilder(
      collectionName as string,
      // I think TS is mad that we're not passing the generic type down to the schema
      // this.schema is of type Models<any, any>, collection query is expecting us to use the generic type M
      // Passing down the generic touched a lot of things, so we're just ignoring the error for now
      // TODO: ...pretty sure this doesnt exist anymore...can it be removed?
      // @ts-ignore
      this.schema && this.schema[collectionName],
      params
    );
  }

  async createCollection(params: CreateCollectionOperation[1]) {
    await this.transact(async (tx) => {
      await tx.createCollection(params);
    });
  }

  async dropCollection(params: DropCollectionOperation[1]) {
    await this.transact(async (tx) => {
      await tx.dropCollection(params);
    });
  }

  async renameAttribute(params: RenameAttributeOperation[1]) {
    await this.transact(async (tx) => {
      await tx.renameAttribute(params);
    });
  }

  async addAttribute(params: AddAttributeOperation[1]) {
    await this.transact(async (tx) => {
      await tx.addAttribute(params);
    });
  }

  async dropAttribute(params: DropAttributeOperation[1]) {
    await this.tripleStore.transact(async (tripTx) => {
      const tx = new DBTransaction(tripTx);
      await tx.dropAttribute(params);
    });
  }

  private async applyRemoteTransaction(operations: DBOperation[]) {
    await this.tripleStore.transact(async (tripTx) => {
      const tx = new DBTransaction(tripTx);
      for (const operation of operations) {
        switch (operation[0]) {
          case 'create_collection':
            await tx.createCollection(operation[1]);
            break;
          case 'drop_collection':
            await tx.dropCollection(operation[1]);
            break;
          case 'rename_attribute':
            await tx.renameAttribute(operation[1]);
            break;
          case 'add_attribute':
            await tx.addAttribute(operation[1]);
            break;
          case 'drop_attribute':
            await tx.dropAttribute(operation[1]);
            break;
          default:
            throw new InvalidMigrationOperationError(
              `The operation ${operation[0]} is not recognized.`
            );
        }
      }
    });
  }

  async migrate(migrations: Migration[], direction: 'up' | 'down') {
    for (const migration of migrations) {
      const tripleSchema = await this.tripleStore.readSchema();
      const dbVersion = tripleSchema?.version ?? 0;
      if (canMigrate(migration, direction, dbVersion)) {
        try {
          await this.applyRemoteTransaction(migration[direction]);
        } catch (e) {
          console.error(
            `Error applying ${direction} migration with verison`,
            migration.version,
            e
          );
          throw e;
        }
        // TODO: move this into the transaction
        await this.tripleStore.updateMetadataTuples([
          [
            '_schema',
            ['version'],
            direction === 'up' ? migration.version : migration.parent,
          ],
        ]);
      } else {
        console.info('skipping migration', migration);
      }
    }
  }

  async getCollectionStats() {
    const collectionMetaTriples = await this.tripleStore.findByAttribute([
      '_collection',
    ]);
    // Aggregates each collection my entity count
    const stats = collectionMetaTriples.reduce((acc, t) => {
      const collectionName = t.value;
      if (!acc.has(collectionName)) {
        acc.set(collectionName, 0);
      }
      acc.set(collectionName, acc.get(collectionName) + 1);
      return acc;
    }, new Map());
    return stats;
  }

  async clear() {
    await this.tripleStore.clear();
  }
}

function canMigrate(
  migration: Migration,
  direction: 'up' | 'down',
  dbVersion: number
) {
  if (direction === 'up') {
    return migration.parent === dbVersion;
  } else {
    return migration.version === dbVersion;
  }
}

function transformTripleAttribute(
  triples: TripleRow[],
  attribute: string[],
  newAttribute: string[]
) {
  // At some point this may not work for all data types, but for now it does
  return triples.map<TripleRow>((triple) => {
    const fullAttribute = [...triple.attribute];
    fullAttribute.splice(0, attribute.length, ...newAttribute);
    return { ...triple, attribute: fullAttribute };
  });
}

const ID_SEPARATOR = '#';

function validateExternalId(id: string): Error | undefined {
  if (String(id).includes(ID_SEPARATOR)) {
    return new InvalidEntityIdError(id, `Id cannot include ${ID_SEPARATOR}.`);
  }
  return;
}

export function appendCollectionToId(collectionName: string, id: string) {
  return `${collectionName}${ID_SEPARATOR}${id}`;
}

export function splitIdParts(id: string): [collectionName: string, id: string] {
  const parts = id.split(ID_SEPARATOR);
  if (parts.length !== 2) {
    throw new InvalidInternalEntityIdError(
      `Malformed ID: ${id} should only include one separator(${ID_SEPARATOR})`
    );
  }
  return [parts[0], parts[1]];
}

export function stripCollectionFromId(id: string): string {
  const [_collection, entityId] = splitIdParts(id);
  return entityId;
}

function replaceVariablesInFilterStatements<M extends Model<any> | undefined>(
  statements: QueryWhere<M>,
  variables: Record<string, any>
): QueryWhere<M> {
  return statements.map((filter) => {
    if (!(filter instanceof Array)) {
      filter.filters = replaceVariablesInFilterStatements(
        filter.filters,
        variables
      );
      return filter;
    }
    if (typeof filter[2] !== 'string' || !filter[2].startsWith('$'))
      return filter;
    const varValue = variables[filter[2].slice(1)];
    if (!varValue) throw new SessionVariableNotFoundError(filter[2]);
    return [filter[0], filter[1], varValue] as FilterStatement<M>;
  });
}
