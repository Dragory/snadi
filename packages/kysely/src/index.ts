import { EntityDefinition, ManyRelationship, MappableInputType, MappableOutputType, OneRelationship, RelationsToLoad, WithLoadedRelations, loadRelationsForArray, loadRelationsForEntity, mapArrayToEntity, mapToEntity } from "@snadi/core";
import { DeleteQueryBuilder, DeleteResult, Kysely, SelectQueryBuilder, TransactionBuilder, UpdateQueryBuilder, UpdateResult } from "kysely";

type Optional<T extends object> = {
  [K in keyof T]?: T[K];
};

type Awaitable<T> = T | Promise<T>;

export type KyselyEntityDefinition = EntityDefinition & {
  tableName: string;
  primaryKey?: string;

  toRow?: (data: any) => any;
};

export type ValidKyselyEntityDefinition<DB> = KyselyEntityDefinition & {
  tableName: keyof DB & string;
};

type ToRowInput<EntityDef extends KyselyEntityDefinition> = EntityDef["toRow"] extends (data: any) => any ? Parameters<EntityDef["toRow"]>[0] : any;

type CreateResult<EntityDef extends KyselyEntityDefinition> = EntityDef["primaryKey"] extends string ? MappableOutputType<EntityDef> : null;

export type EntitiesToKyselyDatabase<Entities extends KyselyEntityDefinition> = {
  [Entity in Entities as Entity["tableName"]]: MappableInputType<Entity>;
};

export class KyselyOrm<DB> {
  kysely: Kysely<DB>;

  constructor(kysely: Kysely<DB>) {
    this.kysely = kysely;
  }

  getAll<EntityDef extends ValidKyselyEntityDefinition<DB>>(entityDef: EntityDef): Promise<Array<MappableOutputType<EntityDef>>>;
  getAll<EntityDef extends ValidKyselyEntityDefinition<DB>, Relations extends RelationsToLoad>(entityDef: EntityDef, relations: Relations): Promise<Array<WithLoadedRelations<MappableOutputType<EntityDef>, Relations>>>;
  async getAll(entityDef: ValidKyselyEntityDefinition<DB>, relations?: RelationsToLoad) {
    return this.loadMany(
      entityDef,
      this.kysely.selectFrom(entityDef.tableName).selectAll().execute(),
      relations as RelationsToLoad,
    );
  }

  getMany<
    EntityDef extends ValidKyselyEntityDefinition<DB>,
    QB extends SelectQueryBuilder<DB, EntityDef["tableName"], {}>,
  >(
    entityDef: EntityDef,
    builder: (qb: QB) => QB,
  ): Promise<Array<MappableOutputType<EntityDef>>>;
  getMany<
    EntityDef extends ValidKyselyEntityDefinition<DB>,
    QB extends SelectQueryBuilder<DB, EntityDef["tableName"], {}>,
    Relations extends RelationsToLoad
  >(
    entityDef: EntityDef,
    builder: (qb: QB) => QB,
    relations: Relations
  ): Promise<Array<WithLoadedRelations<MappableOutputType<EntityDef>, Relations>>>;
  async getMany<QB extends SelectQueryBuilder<DB, keyof DB & string, {}>>(
    entityDef: ValidKyselyEntityDefinition<DB>,
    builder: (qb: QB) => QB,
    relations?: RelationsToLoad,
  ) {
    return this.loadMany(
      entityDef,
      builder(this.kysely.selectFrom(entityDef.tableName).selectAll(entityDef.tableName as any) as unknown as QB).execute(),
      relations as RelationsToLoad,
    );
  }

  getOne<
    EntityDef extends ValidKyselyEntityDefinition<DB>,
    QB extends SelectQueryBuilder<DB, EntityDef["tableName"], {}>,
  >(
    entityDef: EntityDef,
    builder: (qb: QB) => QB,
  ): Promise<MappableOutputType<EntityDef> | null>;
  getOne<
    EntityDef extends ValidKyselyEntityDefinition<DB>,
    QB extends SelectQueryBuilder<DB, EntityDef["tableName"], {}>,
    Relations extends RelationsToLoad,
  >(
    entityDef: EntityDef,
    builder: (qb: QB) => QB,
    relations: Relations,
  ): Promise<WithLoadedRelations<MappableOutputType<EntityDef>, Relations> | null>;
  async getOne<QB extends SelectQueryBuilder<DB, keyof DB & string, {}>>(
    entityDef: ValidKyselyEntityDefinition<DB>,
    builder: (qb: QB) => QB,
    relations?: RelationsToLoad,
  ) {
    return this.loadOne(
      entityDef,
      builder(this.kysely.selectFrom(entityDef.tableName).selectAll(entityDef.tableName as any) as unknown as QB).executeTakeFirst(),
      relations as RelationsToLoad,
    );
  }

  async create<EntityDef extends ValidKyselyEntityDefinition<DB>>(
    entityDef: EntityDef,
    data: ToRowInput<EntityDef>,
  ): Promise<CreateResult<EntityDef>> {
    const dataToInsert = entityDef.toRow ? await entityDef.toRow(data) : data;
    const returning = entityDef.primaryKey ?? "*";
    const inserted = await this.kysely
      .insertInto(entityDef.tableName)
      .values(dataToInsert)
      // See note: https://kysely-org.github.io/kysely-apidoc/classes/InsertQueryBuilder.html#returning
      // Avoiding this bug: https://sqlite.org/forum/forumpost/033daf0b32
      .returning(returning === "*" ? returning : `${returning} as ${returning}` as any)
      .executeTakeFirst();
    return (entityDef.primaryKey
      ? this.getOne(entityDef, qb => qb.where(entityDef.primaryKey! as any, "=", inserted![entityDef.primaryKey!]))
      : null) as CreateResult<EntityDef>;
  }

  async createMany<EntityDef extends ValidKyselyEntityDefinition<DB>>(entityDef: EntityDef, arr: Array<ToRowInput<EntityDef>>): Promise<void> {
    const arrayToInsert = entityDef.toRow
      ? await Promise.all(arr.map(data => entityDef.toRow!(data)))
      : arr;
    await this.kysely.insertInto(entityDef.tableName).values(arrayToInsert).execute();
  }

  async update<
    EntityDef extends ValidKyselyEntityDefinition<DB>,
    QB extends UpdateQueryBuilder<DB, EntityDef["tableName"], EntityDef["tableName"], UpdateResult>,
  >(
    entityDef: EntityDef,
    builder: (qb: QB) => QB,
    fieldsToUpdate: Optional<ToRowInput<EntityDef>>,
  ): Promise<void> {
    const qb = builder(this.kysely.updateTable(entityDef.tableName) as unknown as QB);
    await qb.set(fieldsToUpdate).execute();
  }

  async delete<EntityDef extends ValidKyselyEntityDefinition<DB>>(
    entityDef: EntityDef,
    builder: (qb: DeleteQueryBuilder<DB, EntityDef["tableName"], DeleteResult>) => DeleteQueryBuilder<DB, EntityDef["tableName"], DeleteResult>,
  ): Promise<void> {
    const qb = builder(this.kysely.deleteFrom(entityDef.tableName) as DeleteQueryBuilder<DB, EntityDef["tableName"], DeleteResult>);
    await qb.execute();
  }

  async transaction<T>(
    fn: (orm: KyselyOrm<DB>) => T,
    config?: (builder: TransactionBuilder<DB>) => TransactionBuilder<DB>,
  ): Promise<T> {
    const transaction = config ? config(this.kysely.transaction()) : this.kysely.transaction();
    return transaction.execute(async (trx) => {
      const trxOrm = new KyselyOrm(trx);
      return fn(trxOrm);
    });
  }

  loadOne<EntityDef extends ValidKyselyEntityDefinition<DB>>(
    entityDef: EntityDef,
    promise: Awaitable<MappableInputType<EntityDef> | null>,
  ): Promise<MappableOutputType<EntityDef> | null>;
  loadOne<EntityDef extends ValidKyselyEntityDefinition<DB>>(
    entityDef: EntityDef,
    promise: Awaitable<MappableInputType<EntityDef>>,
  ): Promise<MappableOutputType<EntityDef>>;
  loadOne<EntityDef extends ValidKyselyEntityDefinition<DB>, Relations extends RelationsToLoad>(
    entityDef: EntityDef,
    promise: Awaitable<MappableInputType<EntityDef> | null>,
    relations: Relations,
  ): Promise<WithLoadedRelations<MappableOutputType<EntityDef>, Relations> | null>;
  loadOne<EntityDef extends ValidKyselyEntityDefinition<DB>, Relations extends RelationsToLoad>(
    entityDef: EntityDef,
    promise: Awaitable<MappableInputType<EntityDef>>,
    relations: Relations,
  ): Promise<WithLoadedRelations<MappableOutputType<EntityDef>, Relations>>;
  async loadOne(
    entityDef: ValidKyselyEntityDefinition<DB>,
    promise: Awaitable<any>,
    relations?: RelationsToLoad,
  ) {
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

  loadMany<EntityDef extends ValidKyselyEntityDefinition<DB>>(
    entityDef: EntityDef,
    promise: Awaitable<Array<MappableInputType<EntityDef>>>,
  ): Promise<Array<MappableOutputType<EntityDef>>>;
  loadMany<EntityDef extends ValidKyselyEntityDefinition<DB>, Relations extends RelationsToLoad>(
    entityDef: EntityDef,
    promise: Awaitable<Array<MappableInputType<EntityDef>>>,
    relations: RelationsToLoad,
  ): Promise<Array<MappableOutputType<EntityDef>>>;
  async loadMany(
    entityDef: ValidKyselyEntityDefinition<DB>,
    promise: Awaitable<any>,
    relations?: RelationsToLoad,
  ) {
    const rows = await promise;
    if (! Array.isArray(rows)) {
      throw new Error("load function of loadMany() should return an array of rows, got a non-array instead");
    }
    const entity = await mapArrayToEntity(entityDef, rows);
    return relations ? loadRelationsForArray(entity, relations) : entity;
  }
}

export function createKyselyOrm<DB>(kysely: Kysely<DB>): KyselyOrm<DB> {
  return new KyselyOrm(kysely);
}

export function hasOne<
  LocalEntityDef extends KyselyEntityDefinition,
  OtherEntityDef extends KyselyEntityDefinition,
>(
  localEntityDef: LocalEntityDef, // Just here for type hints
  localField: keyof MappableOutputType<LocalEntityDef>,
  otherEntityDef: OtherEntityDef,
  otherField: keyof MappableOutputType<OtherEntityDef>,
): (orm: KyselyOrm<any>) => OneRelationship<LocalEntityDef, OtherEntityDef> {
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
  LocalEntityDef extends KyselyEntityDefinition,
  OtherEntityDef extends KyselyEntityDefinition,
>(
  localEntityDef: LocalEntityDef, // Just here for type hints
  localField: keyof MappableOutputType<LocalEntityDef>,
  otherEntityDef: OtherEntityDef,
  otherField: keyof MappableOutputType<OtherEntityDef>,
): (orm: KyselyOrm<any>) => ManyRelationship<LocalEntityDef, OtherEntityDef> {
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