import { EntityDefinition, ManyRelationship, MappableInputType, MappableOutputType, OneRelationship, RelationsToLoad, WithLoadedRelations, loadRelationsForArray, loadRelationsForEntity, mapArrayToEntity, mapToEntity } from "@snadi/core";
import { DeleteQueryBuilder, DeleteResult, InsertQueryBuilder, InsertResult, Kysely, SelectQueryBuilder, TransactionBuilder, UpdateQueryBuilder, UpdateResult, sql } from "kysely";

type Awaitable<T> = T | Promise<T>;

export type SnadiKyselyEntityDefinition = EntityDefinition & {
  tableName: string;
  toInsert: (data: any) => Awaitable<any>;
  toUpdate: (data: any) => Awaitable<any>;
};

export type ValidSnadiKyselyEntityDefinition<DB> = SnadiKyselyEntityDefinition & {
  tableName: keyof DB & string;
};

export type InsertInput<EntityDef extends SnadiKyselyEntityDefinition> = EntityDef["toInsert"] extends (data: infer I) => any ? I : never;
export type UpdateInput<EntityDef extends SnadiKyselyEntityDefinition> = EntityDef["toUpdate"] extends (data: infer I) => any ? I : never;

export type EntitiesToKyselyDatabase<Entities extends SnadiKyselyEntityDefinition> = {
  [Entity in Entities as Entity["tableName"]]: MappableInputType<Entity>;
};

function asyncMap<T, O>(arr: T[], fn: (t: T) => O): Promise<O[]> {
  return Promise.all(arr.map(fn));
}

export class SnadiKyselyOrm<DB> {
  kysely: Kysely<DB>;

  constructor(kysely: Kysely<DB>) {
    this.kysely = kysely;
  }

  async getAll<
    EntityDef extends ValidSnadiKyselyEntityDefinition<DB>,
    Relations extends RelationsToLoad | undefined,
  >(
    entityDef: EntityDef,
    relations?: Relations,
  ) {
    return this.loadMany(
      entityDef,
      this.kysely.selectFrom(entityDef.tableName).selectAll().execute(),
      relations,
    );
  }
  async getMany<
    EntityDef extends ValidSnadiKyselyEntityDefinition<DB>,
    Relations extends RelationsToLoad | undefined,
  >(
    entityDef: EntityDef,
    builder: (qb: SelectQueryBuilder<DB, EntityDef["tableName"], {}>) => SelectQueryBuilder<DB, EntityDef["tableName"], {}>,
    relations?: Relations,
  ) {
    return this.loadMany(
      entityDef,
      builder(this.kysely.selectFrom(entityDef.tableName).selectAll(entityDef.tableName as any) as SelectQueryBuilder<DB, EntityDef["tableName"], {}>).execute(),
      relations,
    );
  }

  async getOne<
    EntityDef extends ValidSnadiKyselyEntityDefinition<DB>,
    Relations extends RelationsToLoad | undefined,
  >(
    entityDef: EntityDef,
    builder: (qb: SelectQueryBuilder<DB, EntityDef["tableName"], {}>) => SelectQueryBuilder<DB, EntityDef["tableName"], {}>,
    relations?: Relations,
  ) {
    return this.loadOne(
      entityDef,
      builder(this.kysely.selectFrom(entityDef.tableName).selectAll(entityDef.tableName as any) as SelectQueryBuilder<DB, EntityDef["tableName"], {}>).executeTakeFirst(),
      relations,
    );
  }

  async insert<EntityDef extends ValidSnadiKyselyEntityDefinition<DB>>(
    entityDef: EntityDef,
    data: InsertInput<EntityDef> | Array<InsertInput<EntityDef>>,
    builder?: (qb: InsertQueryBuilder<DB, EntityDef["tableName"], {}>) => InsertQueryBuilder<DB, EntityDef["tableName"], {}>,
  ): Promise<InsertResult> {
    const dataToInsert = Array.isArray(data)
      ? await asyncMap(data, d => entityDef.toInsert(d))
      : await entityDef.toInsert(data);
    let query: InsertQueryBuilder<DB, string & keyof DB, any> = this.kysely
      .insertInto(entityDef.tableName)
      .values(dataToInsert);
    if (builder) {
      query = builder(query);
    }
    return query.executeTakeFirst();
  }

  async update<
    EntityDef extends ValidSnadiKyselyEntityDefinition<DB>,
    QB extends UpdateQueryBuilder<DB, EntityDef["tableName"], EntityDef["tableName"], UpdateResult>,
  >(
    entityDef: EntityDef,
    builder: (qb: QB) => QB,
    data: UpdateInput<EntityDef>,
  ): Promise<UpdateResult> {
    const qb = builder(this.kysely.updateTable(entityDef.tableName) as unknown as QB);
    const dataToUpdate = await entityDef.toUpdate(data);
    return qb.set(dataToUpdate).executeTakeFirst();
  }

  async delete<EntityDef extends ValidSnadiKyselyEntityDefinition<DB>>(
    entityDef: EntityDef,
    builder: (qb: DeleteQueryBuilder<DB, EntityDef["tableName"], DeleteResult>) => DeleteQueryBuilder<DB, EntityDef["tableName"], DeleteResult>,
  ): Promise<DeleteResult> {
    const qb = builder(this.kysely.deleteFrom(entityDef.tableName) as DeleteQueryBuilder<DB, EntityDef["tableName"], DeleteResult>);
    return qb.executeTakeFirst();
  }

  async transaction<T>(
    fn: (orm: SnadiKyselyOrm<DB>) => T,
    config?: (builder: TransactionBuilder<DB>) => TransactionBuilder<DB>,
  ): Promise<T> {
    const transaction = config ? config(this.kysely.transaction()) : this.kysely.transaction();
    return transaction.execute(async (trx) => {
      const trxOrm = new SnadiKyselyOrm(trx);
      return fn(trxOrm);
    });
  }

  async loadOne<
    EntityDef extends ValidSnadiKyselyEntityDefinition<DB>,
    Relations extends RelationsToLoad | undefined,
  >(
    entityDef: EntityDef,
    promise: Awaitable<MappableInputType<EntityDef>>,
    relations?: Relations,
  ): Promise<WithLoadedRelations<MappableOutputType<EntityDef>, Relations> | null> {
    const row = await promise;
    if (Array.isArray(row)) {
      throw new Error("load function of loadOne() should return a single row, got an array instead");
    }
    if (row == null) {
      return null;
    }
    const entity = await mapToEntity(entityDef, row);
    return relations ? loadRelationsForEntity(entity, relations) : entity;
  }

  async loadMany<
    EntityDef extends ValidSnadiKyselyEntityDefinition<DB>,
    Relations extends RelationsToLoad | undefined,
  >(
    entityDef: EntityDef,
    promise: Awaitable<Array<MappableInputType<EntityDef>>>,
    relations?: Relations,
  ): Promise<Array<WithLoadedRelations<MappableOutputType<EntityDef>, Relations>>> {
    const rows = await promise;
    if (! Array.isArray(rows)) {
      throw new Error("load function of loadMany() should return an array of rows, got a non-array instead");
    }
    const entities = await mapArrayToEntity(entityDef, rows);
    return relations ? loadRelationsForArray(entities, relations) : entities;
  }
}

export function createKyselyOrm<DB>(kysely: Kysely<DB>): SnadiKyselyOrm<DB> {
  return new SnadiKyselyOrm(kysely);
}

export function hasOne<
  LocalEntityDef extends SnadiKyselyEntityDefinition,
  OtherEntityDef extends SnadiKyselyEntityDefinition,
>(
  localEntityDef: LocalEntityDef, // Just here for type hints
  localField: keyof MappableOutputType<LocalEntityDef>,
  otherEntityDef: OtherEntityDef,
  otherField: keyof MappableOutputType<OtherEntityDef>,
): (orm: SnadiKyselyOrm<any>) => OneRelationship<LocalEntityDef, OtherEntityDef> {
  return (orm) => {
    return {
      otherEntity: otherEntityDef,
      load: async (localEntities) => {
        const keys = new Set(localEntities.map(e => e[localField]));
        if (keys.size === 0) {
          return [];
        }
        return orm.kysely
          .selectFrom(otherEntityDef.tableName)
          .where(otherField as string, "in", Array.from(keys))
          .selectAll()
          .execute();
      },
      attach: (otherEntities) => {
        const otherEntitiesByKey = new Map<any, MappableOutputType<OtherEntityDef>>();
        for (const otherEntity of otherEntities) {
          otherEntitiesByKey.set(otherEntity[otherField], otherEntity);
        }
        return (localEntity) => {
          return otherEntitiesByKey.get(localEntity[localField]) ?? null;
        };
      },
    };
  };
}

export function hasMany<
  LocalEntityDef extends SnadiKyselyEntityDefinition,
  OtherEntityDef extends SnadiKyselyEntityDefinition,
>(
  localEntityDef: LocalEntityDef, // Just here for type hints
  localField: keyof MappableOutputType<LocalEntityDef>,
  otherEntityDef: OtherEntityDef,
  otherField: keyof MappableOutputType<OtherEntityDef>,
): (orm: SnadiKyselyOrm<any>) => ManyRelationship<LocalEntityDef, OtherEntityDef> {
  return (orm) => {
    return {
      otherEntity: otherEntityDef,
      load: async (localEntities) => {
        const keys = new Set(localEntities.map(e => e[localField]));
        if (keys.size === 0) {
          return [];
        }
        return orm.kysely
          .selectFrom(otherEntityDef.tableName)
          .where(otherField as string, "in", Array.from(keys))
          .selectAll()
          .execute();
      },
      attach: (otherEntities) => {
        const otherEntitiesByKey = new Map<any, Array<MappableOutputType<OtherEntityDef>>>();
        for (const otherEntity of otherEntities) {
          if (! otherEntitiesByKey.has(otherEntity[otherField])) {
            otherEntitiesByKey.set(otherEntity[otherField], []);
          }
          otherEntitiesByKey.get(otherEntity[otherField])!.push(otherEntity);
        }
        return (localEntity) => {
          return otherEntitiesByKey.get(localEntity[localField]) ?? [];
        };
      },
    };
  };
}
