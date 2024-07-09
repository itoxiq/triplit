import { describe, expect, it, beforeEach, beforeAll, vi } from 'vitest';
import {
  diffSchemas,
  getSchemaDiffIssues,
  getBackwardsIncompatibleEdits,
} from '../src/schema/diff.js';
import { Schema as S } from '../src/schema/builder.js';
import DB, { DBTransaction, Models } from '../src/index.js';

function wrapSchema(definition: any) {
  return {
    version: 0,
    collections: { stressTest: { schema: S.Schema(definition) } },
  };
}

const stressTest = {
  id: S.Id(),
  string: S.String(),
  number: S.Number(),
  boolean: S.Boolean(),
  date: S.Date(),
  stringSet: S.Set(S.String()),
  numberSet: S.Set(S.Number()),
  booleanSet: S.Set(S.Boolean()),
  dateSet: S.Set(S.Date()),
  record: S.Record({}),
  recordWithKeys: S.Record({
    string: S.String(),
    number: S.Number(),
    boolean: S.Boolean(),
    date: S.Date(),
  }),
  optionalString: S.Optional(S.String()),
  optionalNumber: S.Optional(S.Number()),
  optionalBoolean: S.Optional(S.Boolean()),
  optionalDate: S.Optional(S.Date()),
  optionalStringSet: S.Optional(S.Set(S.String())),
  optionalNumberSet: S.Optional(S.Set(S.Number())),
  optionalBooleanSet: S.Optional(S.Set(S.Boolean())),
  optionalDateSet: S.Optional(S.Set(S.Date())),
  optionalRecord: S.Optional(S.Record({})),
  nullableString: S.String({ nullable: true }),
  nullableNumber: S.Number({ nullable: true }),
  nullableBoolean: S.Boolean({ nullable: true }),
  nullableDate: S.Date({ nullable: true }),
  nullableStringSet: S.Set(S.String(), { nullable: true }),
  nullableNumberSet: S.Set(S.Number(), { nullable: true }),
  nullableBooleanSet: S.Set(S.Boolean(), { nullable: true }),
  nullableDateSet: S.Set(S.Date(), { nullable: true }),
};

const changed = { ...stressTest };
// setting a nullable to non-nullable DANGEROUS
changed.nullableDate = S.Date();

// setting nullable and adding a default SAFE
changed.date = S.Date({ nullable: true, default: S.Default.now() });

// making something optional non-optional DANGEROUS
changed.optionalBoolean = S.Boolean();

// making something non-optional, optional SAFE
changed.boolean = S.Optional(S.Boolean());

// changing a type DANGEROUS
changed.number = S.Set(S.Number());

// changing a sets item type DANGEROUS
changed.booleanSet = S.Set(S.Number());

// changing a record item's type DANGEROUS
changed.recordWithKeys = S.Record({
  string: S.Number(),
  number: S.Number(),
  boolean: S.Boolean(),
  date: S.Date(),
});

// added an attribute that's not optional, DANGEROUS
changed.new = S.String();

// added an attribute that's optional, SAFE
changed.optionalNew = S.Optional(S.String());

// deleting an attribute DANGEROUS
delete changed.record;

const stressTestSchema = wrapSchema(stressTest);

describe('Schema diffing', () => {
  it('can diff a big schema', () => {
    const result = diffSchemas(stressTestSchema, stressTestSchema);
    expect(result.length).toBe(0);
  });
  it('can diff a missing attribute', () => {
    const lackingAnAttribute = { ...stressTest };
    delete lackingAnAttribute.id;
    const result = diffSchemas(
      stressTestSchema,
      wrapSchema(lackingAnAttribute)
    );
    expect(result).toStrictEqual([
      {
        _diff: 'collectionAttribute',
        collection: 'stressTest',
        type: 'delete',
        attribute: ['id'],
        metadata: {
          type: 'string',
          options: { nullable: false, default: { args: null, func: 'uuid' } },
          optional: false,
        },
      },
    ]);
    const reverseResult = diffSchemas(
      wrapSchema(lackingAnAttribute),
      stressTestSchema
    );
    expect(reverseResult).toStrictEqual([
      {
        _diff: 'collectionAttribute',
        collection: 'stressTest',
        type: 'insert',
        attribute: ['id'],
        metadata: {
          type: 'string',
          options: { nullable: false, default: { args: null, func: 'uuid' } },
          optional: false,
        },
        isNewCollection: false,
      },
    ]);
  });
  it('can diff multiple changed attributes', () => {
    const noAttributes = {};
    const result = diffSchemas(stressTestSchema, wrapSchema(noAttributes));
    for (const res of result) {
      expect(stressTest[res.attribute[0]]).toBeTruthy();
      expect(res.collection).toBe('stressTest');
      expect(res.type).toStrictEqual('delete');
    }
    expect(result.length).toBe(Object.keys(stressTest).length);
    const reverseResult = diffSchemas(
      wrapSchema(noAttributes),
      stressTestSchema
    );
    expect(reverseResult.length).toBe(Object.keys(stressTest).length);
    for (const res of reverseResult) {
      expect(stressTest[res.attribute[0]]).toBeTruthy();
      expect(res.collection).toBe('stressTest');
      expect(res.type).toStrictEqual('insert');
    }
  });
  it('can diff changed optional / not optional changes', () => {
    const result = diffSchemas(stressTestSchema, wrapSchema(changed));
    const resultsMap = result.reduce((prev, curr) => {
      return { ...prev, [curr.attribute[0]]: curr };
    }, {});
    expect(resultsMap.number.changes.type).toBe('set');
    expect(resultsMap.boolean.changes.optional).toBe(true);
    expect(resultsMap.optionalBoolean.changes.optional).toBe(false);
    expect(resultsMap.nullableDate.changes.options.nullable).toBe(false);
    expect(resultsMap.date.changes.options).toStrictEqual({
      nullable: true,
      default: S.Default.now(),
    });
  });
  it('can diff schemas with different collections', () => {
    const schemaA = {
      version: 0,
      collections: { first: { schema: S.Schema({ id: S.Id() }) } },
    };
    const schemaB = {
      version: 0,
      collections: {
        first: { schema: S.Schema({ id: S.Id() }) },
        second: { schema: S.Schema({ id: S.Id() }) },
      },
    };
    const diff = diffSchemas(schemaB, schemaA);
    expect(diff).toStrictEqual([
      {
        _diff: 'collectionAttribute',
        collection: 'second',
        type: 'delete',
        attribute: ['id'],
        metadata: {
          options: { nullable: false, default: { args: null, func: 'uuid' } },
          type: 'string',
          optional: false,
        },
      },
    ]);
    const reverseDiff = diffSchemas(schemaA, schemaB);
    expect(reverseDiff).toStrictEqual([
      {
        _diff: 'collectionAttribute',
        collection: 'second',
        type: 'insert',
        attribute: ['id'],
        metadata: {
          type: 'string',
          options: { nullable: false, default: { args: null, func: 'uuid' } },
          optional: false,
        },
        isNewCollection: true,
      },
    ]);
  });
});

describe('detecting dangerous edits', () => {
  it('can detect dangerous edits', () => {
    const destructiveEdits = getBackwardsIncompatibleEdits(
      diffSchemas(stressTestSchema, wrapSchema(changed))
    );
    const attributesChangedDangerously = new Set([
      'number',
      'optionalBoolean',
      'nullableDate',
      'booleanSet',
      'recordWithKeys',
      'new',
      'record',
    ]);
    for (const edit of destructiveEdits) {
      expect(attributesChangedDangerously.has(edit.context.attribute[0])).toBe(
        true
      );
    }
    expect(destructiveEdits.length).toBe(7);
  });
  it('can evaluate if an edit will lead to data corruption', async () => {
    const original = wrapSchema({
      // IN THE REVERSE DIRECTION (i.e. from different to original)
      id: S.Id(), // safe
      string: S.String(), // dangerous, but ok based on DB
      number: S.Number(), // dangerous, but ok based on DB
      boolean: S.Boolean(), // safe-ish. You have to update client code to handle the old default
      date: S.Date(), // dangerous, but ok based on DB
      changedType: S.String(), // dangerous
      missingAttribute: S.String(), // dangerous, but ok based on DB
      setString: S.Set(S.String()), // dangerous, but ok based on DB
      setNumber: S.Set(S.Number()), // dangerous, never allowed
      emptyRecord: S.Record({}), // dangerous
      filledRecord: S.Record({ a: S.String(), b: S.Number(), c: S.Boolean() }), // 3x dangerous
    });
    const different = wrapSchema({
      id: S.Id(), // safe
      string: S.Optional(S.String()), // safe
      number: S.Number({ nullable: true }), // safe
      boolean: S.Boolean({ default: true }), // safe
      date: S.Date({ default: S.Default.now(), nullable: true }), // safe
      changedType: S.Number(), // dangerous, but ok based on DB
      //delete missingAttribute, dangerous, but ok based on DB
      setString: S.Set(S.String(), { nullable: true }), // safe
      setNumber: S.Set(S.Boolean()), // dangerous, but ok based on DB
      emptyRecord: S.Record({ a: S.String() }), // dangerous, but ok based on DB
      filledRecord: S.Record({ b: S.Number(), c: S.String(), d: S.String() }), // 3 x dangerous, but ok based on DB
    });
    const db = new DB({ schema: original });
    let results;
    await db.transact(async (tx) => {
      results = await getSchemaDiffIssues(tx, diffSchemas(original, different));
      // happiest case, nothing in the database
      // TODO: changing types should be allowed with empty databases? tbd
      expect(
        results.map(({ violatesExistingData }) => violatesExistingData)
      ).toStrictEqual([false, false, false, false, false, false, false]);
    });

    let reverseResults;
    await db.transact(async (tx) => {
      reverseResults = await getSchemaDiffIssues(
        tx,
        diffSchemas(different, original)
      );

      expect(
        reverseResults.map(({ violatesExistingData }) => violatesExistingData)
      ).toStrictEqual([
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
        false,
      ]);
    });

    await db.insert('stressTest', {
      string: 'string',
      number: 1,
      boolean: false,
      date: new Date(),
      changedType: 'string',
      missingAttribute: 'string',
      setString: new Set(['string']),
      setNumber: new Set([1]),
      emptyRecord: {},
      filledRecord: { a: 'string', b: 1, c: true },
    });

    await db.transact(async (tx) => {
      results = await getSchemaDiffIssues(tx, diffSchemas(original, different));
      expect(
        results.map(({ violatesExistingData }) => violatesExistingData)
      ).toStrictEqual([true, true, true, true, true, true, true]);
    });

    const db2 = new DB({ schema: different });
    await db2.insert('stressTest', {
      string: 'string',
      number: 1,
      boolean: false,
      date: new Date(),
      changedType: 1,
      setString: new Set(['string']),
      setNumber: new Set([false]),
      emptyRecord: { a: 'string' },
      filledRecord: { b: 1, c: 'string', d: 'string' },
    });
    await db2.transact(async (tx) => {
      reverseResults = await getSchemaDiffIssues(
        tx,
        diffSchemas(different, original)
      );
      expect(
        reverseResults.map(({ violatesExistingData }) => violatesExistingData)
      ).toStrictEqual([
        false, // string always has a value
        false, // number always has a value, is not null
        false, // date always has a value
        true, // changedType is not empty
        false, // no entities have setString as null
        true, // setNumber has value, can't change the type
        true, // emptyRecord.a has an entity with a value, can't remove it
        true, // filledRecord.c has a value, can't change the type
        true, // filledRecord.d has a value, can't remove it
        true, // filledRecord.a can't be added, entities already exist
        true, // missingAttribute can't be added without Optional because entities already exist
      ]);
    });
  });

  async function diffEnumAttributes(
    tx: DBTransaction<any>,
    original: any,
    different: any
  ) {
    return await getSchemaDiffIssues(
      tx,
      diffSchemas(
        wrapSchema({ id: S.Id(), enum: original }),
        wrapSchema({ id: S.Id(), enum: different })
      )
    );
  }
  const noEnum = S.String();
  const withEnum = S.String({ enum: ['a', 'b', 'c'] });
  const withDangerouslyChangedEnum = S.String({ enum: ['a', 'b', 'd'] });
  const withSafeChangedEnum = S.String({ enum: ['a', 'b', 'c', 'd'] });

  it('can detect dangerous changes to an enum', async () => {
    const db = new DB({ schema: wrapSchema({ id: S.Id(), enum: noEnum }) });
    await db.transact(async (tx) => {
      const results = await diffEnumAttributes(tx, noEnum, withEnum);
      expect(results[0].violatesExistingData).toBe(false);
    });
    // add a value that's not in the enum
    await db.insert('stressTest', { id: 'test', enum: 'e' });
    await db.transact(async (tx) => {
      const results = await diffEnumAttributes(tx, noEnum, withEnum);
      expect(results[0].violatesExistingData).toBe(true);
    });
    // update the value that's in the enum
    await db.update('stressTest', 'test', (entity) => {
      entity.enum = 'a';
    });
    await db.transact(async (tx) => {
      const results = await diffEnumAttributes(tx, noEnum, withEnum);
      expect(results[0].violatesExistingData).toBe(false);
    });
    // can now update the schema safely
    await db.overrideSchema(wrapSchema({ id: S.Id(), enum: withEnum }));
    await pause(100);
    // changing the enum value to a non-super set is dangerous, but safe in this case
    await db.transact(async (tx) => {
      const results = await diffEnumAttributes(
        tx,
        withEnum,
        withDangerouslyChangedEnum
      );
      expect(results[0].violatesExistingData).toBe(false);
    });
    await db.insert('stressTest', { id: 'test', enum: 'c' });

    // change the enum value to a super set is safe
    await db.transact(async (tx) => {
      const results = await diffEnumAttributes(
        tx,
        withEnum,
        withSafeChangedEnum
      );
      expect(results.length).toBe(0);
    });

    // but it is dangerous when there's not in the new enum
    await db.transact(async (tx) => {
      const results = await diffEnumAttributes(
        tx,
        withEnum,
        withDangerouslyChangedEnum
      );
      expect(results[0].violatesExistingData).toBe(true);
    });

    // going from an enum to no enum is always safe
    await db.transact(async (tx) => {
      const results = await diffEnumAttributes(tx, withEnum, noEnum);
      expect(results.length).toBe(0);
    });
  });
});

function pause(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe('rules', () => {
  it('can detect changes to rules', () => {
    const schemaA = {
      version: 0,
      collections: {
        test: {
          schema: S.Schema({ id: S.Id() }),
          rules: {
            read: { 'can-read': { filter: [false] } },
          },
        },
      },
    };
    const schemaB = {
      version: 0,
      collections: {
        test: {
          schema: S.Schema({ id: S.Id() }),
          rules: {
            read: { 'cant-read': { filter: [false] } },
          },
        },
      } satisfies Models<any, any>,
    };

    const diff = diffSchemas(schemaA, schemaB);
    expect(diff).toStrictEqual([
      {
        _diff: 'collectionRules',
        collection: 'test',
      },
    ]);
  });
  it('can add rules', () => {
    const schemaA = {
      version: 0,
      collections: {
        test: {
          schema: S.Schema({ id: S.Id() }),
        },
      },
    };
    const schemaB = {
      version: 0,
      collections: {
        test: {
          schema: S.Schema({ id: S.Id() }),
          rules: {
            read: { 'cant-read': { filter: [false] } },
          },
        },
      } satisfies Models<any, any>,
    };

    const diff = diffSchemas(schemaA, schemaB);
    expect(diff).toStrictEqual([
      {
        _diff: 'collectionRules',
        collection: 'test',
      },
    ]);
  });
  it('can remove rules', () => {
    const schemaA = {
      version: 0,
      collections: {
        test: {
          schema: S.Schema({ id: S.Id() }),
          rules: {
            read: { 'can-read': { filter: [false] } },
          },
        },
      },
    };
    const schemaB = {
      version: 0,
      collections: {
        test: {
          schema: S.Schema({ id: S.Id() }),
        },
      } satisfies Models<any, any>,
    };

    const diff = diffSchemas(schemaA, schemaB);
    expect(diff).toStrictEqual([
      {
        _diff: 'collectionRules',
        collection: 'test',
      },
    ]);
  });
});

describe('roles', () => {
  it('can detect changes to roles', () => {
    const schemaA = {
      version: 0,
      collections: {
        test: {
          schema: S.Schema({ id: S.Id() }),
        },
      },
      roles: {
        user: { match: { type: 'user' } },
      },
    };
    const schemaB = {
      version: 0,
      collections: {
        test: {
          schema: S.Schema({ id: S.Id() }),
        },
      },
      roles: {
        user: { match: { role: 'user' } },
      },
    };
    const diff = diffSchemas(schemaA, schemaB);
    expect(diff).toStrictEqual([
      {
        _diff: 'roles',
      },
    ]);
  });
  it('can add roles', () => {
    const schemaA = {
      version: 0,
      collections: {
        test: {
          schema: S.Schema({ id: S.Id() }),
        },
      },
    };
    const schemaB = {
      version: 0,
      collections: {
        test: {
          schema: S.Schema({ id: S.Id() }),
        },
      },
      roles: {
        user: { match: { role: 'user' } },
      },
    };
    const diff = diffSchemas(schemaA, schemaB);
    expect(diff).toStrictEqual([
      {
        _diff: 'roles',
      },
    ]);
  });
  it('can remove roles', () => {
    const schemaA = {
      version: 0,
      collections: {
        test: {
          schema: S.Schema({ id: S.Id() }),
        },
      },
      roles: {
        user: { match: { role: 'user' } },
      },
    };
    const schemaB = {
      version: 0,
      collections: {
        test: {
          schema: S.Schema({ id: S.Id() }),
        },
      },
    };
    const diff = diffSchemas(schemaA, schemaB);
    expect(diff).toStrictEqual([
      {
        _diff: 'roles',
      },
    ]);
  });
});

describe('permissions', () => {
  it('can detect changes to permissions', () => {
    const schemaA = {
      version: 0,
      collections: {
        test: {
          schema: S.Schema({ id: S.Id() }),
          permissions: {
            user: {
              read: { filter: [true] },
            },
          },
        },
      } satisfies Models<any, any>,
    };
    const schemaB = {
      version: 0,
      collections: {
        test: {
          schema: S.Schema({ id: S.Id() }),
          permissions: {
            user: {
              read: { filter: [false] },
            },
          },
        },
      } satisfies Models<any, any>,
    };
    const diff = diffSchemas(schemaA, schemaB);
    expect(diff).toStrictEqual([
      {
        _diff: 'collectionPermissions',
        collection: 'test',
      },
    ]);
  });
  it('can add permissions', () => {
    const schemaA = {
      version: 0,
      collections: {
        test: {
          schema: S.Schema({ id: S.Id() }),
        },
      } satisfies Models<any, any>,
    };
    const schemaB = {
      version: 0,
      collections: {
        test: {
          schema: S.Schema({ id: S.Id() }),
          permissions: {
            user: {
              read: { filter: [true] },
            },
          },
        },
      } satisfies Models<any, any>,
    };
    const diff = diffSchemas(schemaA, schemaB);
    expect(diff).toStrictEqual([
      {
        _diff: 'collectionPermissions',
        collection: 'test',
      },
    ]);
  });
  it('can remove permissions', () => {
    const schemaA = {
      version: 0,
      collections: {
        test: {
          schema: S.Schema({ id: S.Id() }),
          permissions: {
            user: {
              read: { filter: [true] },
            },
          },
        },
      } satisfies Models<any, any>,
    };
    const schemaB = {
      version: 0,
      collections: {
        test: {
          schema: S.Schema({ id: S.Id() }),
        },
      } satisfies Models<any, any>,
    };
    const diff = diffSchemas(schemaA, schemaB);
    expect(diff).toStrictEqual([
      {
        _diff: 'collectionPermissions',
        collection: 'test',
      },
    ]);
  });
});
